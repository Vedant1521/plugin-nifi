package io.kestra.plugin.nifi;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Trigger that periodically polls the Apache NiFi Bulletin Board for errors or events and initiates workflow executions.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger workflow executions on Apache NiFi Bulletin Board events",
    description = "Periodically polls the NiFi Bulletin Board and triggers workflow executions when new bulletins matching the configured level are detected."
)
@Plugin(
    examples = {
        @Example(
            title = "Poll Apache NiFi Bulletin Board for ERROR events every minute",
            full = true,
            code = """
                id: nifi_error_alert
                namespace: company.team

                tasks:
                  - id: log_bulletins
                    type: io.kestra.plugin.core.log.Log
                    message: "Discovered {{ trigger.bulletins | length }} NiFi errors: {{ trigger.bulletins }}"

                triggers:
                  - id: nifi_bulletin_trigger
                    type: io.kestra.plugin.nifi.Trigger
                    url: "https://localhost:8443"
                    username: "nifi-admin"
                    password: "{{ secret('NIFI_PASSWORD') }}"
                    sslVerify: false
                    level: "ERROR"
                    interval: PT1M
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, NifiConnectionInterface {
    @NotNull
    @Schema(
        title = "The Apache NiFi base URL",
        description = "The fully qualified URL pointing to your Apache NiFi cluster or instance (e.g., https://localhost:8443 or http://localhost:8080)."
    )
    @PluginProperty(group = "connection")
    private Property<String> url;

    @Schema(
        title = "The username for NiFi authentication"
    )
    @PluginProperty(group = "connection")
    private Property<String> username;

    @Schema(
        title = "The password for NiFi authentication"
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    private Property<String> password;

    @Schema(
        title = "Whether to verify SSL certificates",
        description = "Set to false to disable SSL verification (e.g., for self-signed certificates)."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> sslVerify = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Bulletin level to filter",
        description = "The bulletin level to filter on: DEBUG, INFO, WARNING, or ERROR. WARN is accepted as an alias for WARNING. Defaults to 'ERROR'."
    )
    @PluginProperty(group = "main")
    private Property<Level> level = Property.ofValue(Level.ERROR);

    @Builder.Default
    @Schema(
        title = "Interval between polling checks",
        description = "The duration between each poll to the NiFi Bulletin Board. Defaults to 1 minute (PT1M). Note that NiFi retains at most 5 bulletins per component for up to 5 minutes, so higher polling intervals may miss bulletins under heavy activity."
    )
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofMinutes(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        String token = NifiService.authenticate(this, runContext);

        Level targetLevel = runContext.render(this.level).as(Level.class).orElse(Level.ERROR);
        URI bulletinUri = NifiService.resolveUri(this, runContext, "/flow/bulletin-board");

        HttpRequest request = HttpRequest.builder()
            .uri(bulletinUri)
            .method("GET")
            .build();

        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        KVStore kv = runContext.namespaceKv(flowInfo.namespace());
        String stateKey = "nifi_watermark_" + flowInfo.id().length() + "_" + flowInfo.id() + "_" + context.getTriggerId();

        long currentWatermark = kv.getValue(stateKey)
            .map(v -> Long.parseLong(String.valueOf(v.value())))
            .orElse(-1L);

        long maxId = currentWatermark;
        List<Map<String, Object>> matchingBulletins = new ArrayList<>();

        try (HttpClient client = NifiService.createHttpClient(this, runContext, token)) {
            HttpResponse<String> response = client.request(request, String.class);

            JsonNode rootNode = JacksonMapper.ofJson().readTree(response.getBody());
            JsonNode bulletinsNode = rootNode.path("bulletinBoard").path("bulletins");
            if (!bulletinsNode.isArray()) {
                bulletinsNode = rootNode.path("bulletins");
            }

            if (bulletinsNode.isArray() && !bulletinsNode.isEmpty()) {
                // NiFi bulletin IDs come from an in-memory AtomicLong that starts at 0 upon service restart.
                // If the board has bulletins but the highest ID found is lower than our watermark,
                // NiFi has restarted; reset currentWatermark to -1 to process new bulletins.
                long boardMaxId = -1L;
                for (JsonNode bulletinNode : bulletinsNode) {
                    long id = bulletinNode.path("id").asLong(-1L);
                    if (id == -1L && bulletinNode.has("bulletin")) {
                        id = bulletinNode.path("bulletin").path("id").asLong(-1L);
                    }
                    if (id > boardMaxId) {
                        boardMaxId = id;
                    }
                }

                if (currentWatermark != -1L && boardMaxId != -1L && boardMaxId < currentWatermark) {
                    runContext.logger().info("NiFi restart detected (board max ID {} < stored watermark {}). Resetting bulletin watermark.",
                        boardMaxId, currentWatermark);
                    currentWatermark = -1L;
                    maxId = -1L;
                }

                for (JsonNode bulletinNode : bulletinsNode) {
                    long id = bulletinNode.path("id").asLong(-1L);
                    if (id == -1L && bulletinNode.has("bulletin")) {
                        id = bulletinNode.path("bulletin").path("id").asLong(-1L);
                    }

                    String bulletinLevel = bulletinNode.path("bulletin").path("level").asText("");
                    if (bulletinLevel.isEmpty()) {
                        bulletinLevel = bulletinNode.path("level").asText("");
                    }

                    if (id > maxId) {
                        maxId = id;
                    }

                    if (id > currentWatermark && targetLevel.name().equalsIgnoreCase(normalizeLevel(bulletinLevel))) {
                        matchingBulletins.add(JacksonMapper.toMap(bulletinNode));
                    }
                }
            }
        }

        if (maxId > currentWatermark) {
            kv.put(stateKey, new KVValueAndMetadata(null, String.valueOf(maxId)));
        }

        if (matchingBulletins.isEmpty()) {
            return Optional.empty();
        }

        runContext.logger().info("Found {} new NiFi bulletins matching level '{}'", matchingBulletins.size(), targetLevel.name());

        Output output = Output.builder()
            .bulletins(matchingBulletins)
            .build();

        Execution execution = TriggerService.generateExecution(
            this,
            conditionContext,
            context,
            output
        );

        return Optional.of(execution);
    }

    private static String normalizeLevel(String level) {
        if (level == null) {
            return "";
        }
        String trimmed = level.trim();
        if ("WARN".equalsIgnoreCase(trimmed)) {
            return "WARNING";
        }
        return trimmed.toUpperCase();
    }

    public enum Level {
        DEBUG,
        INFO,
        @JsonAlias("WARN")
        WARNING,
        ERROR;

        @JsonCreator
        public static Level fromString(String value) {
            if (value == null) {
                return null;
            }
            if ("WARN".equalsIgnoreCase(value.trim())) {
                return WARNING;
            }
            return Level.valueOf(value.trim().toUpperCase());
        }
    }

    /**
     * Output containing the list of newly discovered NiFi bulletins.
     */
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of bulletins discovered matching the criteria",
            description = "List of NiFi bulletin objects matching the configured level since the last watermark."
        )
        private final List<Map<String, Object>> bulletins;
    }
}
