package io.kestra.plugin.nifi;

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
import io.kestra.core.storages.StateStore;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
    @PluginProperty
    private Property<String> url;

    @Schema(
        title = "The username for NiFi authentication"
    )
    @PluginProperty
    private Property<String> username;

    @Schema(
        title = "The password for NiFi authentication"
    )
    @PluginProperty(secret = true)
    @ToString.Exclude
    private Property<String> password;

    @Schema(
        title = "Whether to verify SSL certificates",
        description = "Set to false to disable SSL verification (e.g., for self-signed certificates)."
    )
    @Builder.Default
    @PluginProperty
    private Property<Boolean> sslVerify = Property.ofValue(true);

    @Schema(
        title = "Client certificate for mutual TLS (mTLS) authentication",
        description = "The client certificate content or certificate file path used for mutual TLS authentication."
    )
    @PluginProperty(secret = true)
    @ToString.Exclude
    private Property<String> clientCertificate;

    @Builder.Default
    @Schema(
        title = "Bulletin level to filter",
        description = "The bulletin level to filter on (e.g., ERROR, WARN, INFO). Defaults to 'ERROR'."
    )
    @PluginProperty
    private Property<String> level = Property.ofValue("ERROR");

    @Builder.Default
    @Schema(
        title = "Interval between polling checks",
        description = "The duration between each poll to the NiFi Bulletin Board. Defaults to 1 minute (PT1M)."
    )
    @PluginProperty
    private final Duration interval = Duration.ofMinutes(1);

    @SuppressWarnings({"deprecation", "removal"})
    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        String token = NifiService.authenticate(this, runContext);

        String targetLevel = runContext.render(this.level).as(String.class).orElse("ERROR");
        URI bulletinUri = NifiService.resolveUri(this, runContext, "/flow/bulletin-board");

        HttpRequest request = HttpRequest.builder()
            .uri(bulletinUri)
            .method("GET")
            .build();

        StateStore stateStore = runContext.stateStore();
        RunContext.FlowInfo flowInfo = runContext.flowInfo();
        String stateKey = context.getTriggerId() + "_bulletin_watermark";

        Long currentWatermark = -1L;
        try (InputStream is = stateStore.getState(true, flowInfo.tenantId(), flowInfo.namespace(), stateKey)) {
            if (is != null) {
                String val = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
                if (!val.isEmpty()) {
                    currentWatermark = Long.parseLong(val);
                }
            }
        } catch (Exception ignored) {
            // State not initialized yet on first poll
        }

        Long maxId = currentWatermark;
        List<Map<String, Object>> matchingBulletins = new ArrayList<>();

        try (HttpClient client = NifiService.createHttpClient(this, runContext, token)) {
            HttpResponse<String> response = client.request(request, String.class);

            JsonNode rootNode = JacksonMapper.ofJson().readTree(response.getBody());
            JsonNode bulletinsNode = rootNode.path("bulletinBoard").path("bulletins");
            if (!bulletinsNode.isArray()) {
                bulletinsNode = rootNode.path("bulletins");
            }

            if (bulletinsNode.isArray()) {
                for (JsonNode bulletinNode : bulletinsNode) {
                    Long id = bulletinNode.path("id").asLong(-1L);
                    String bulletinLevel = bulletinNode.path("level").asText("");

                    if (id > maxId) {
                        maxId = id;
                    }

                    if (id > currentWatermark && targetLevel.equalsIgnoreCase(bulletinLevel)) {
                        matchingBulletins.add(JacksonMapper.toMap(bulletinNode));
                    }
                }
            }
        }

        if (maxId > currentWatermark) {
            stateStore.putState(
                true,
                flowInfo.tenantId(),
                flowInfo.namespace(),
                stateKey,
                String.valueOf(maxId).getBytes(StandardCharsets.UTF_8)
            );
        }

        if (matchingBulletins.isEmpty()) {
            return Optional.empty();
        }

        runContext.logger().info("Found {} new NiFi bulletins matching level '{}'", matchingBulletins.size(), targetLevel);

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
