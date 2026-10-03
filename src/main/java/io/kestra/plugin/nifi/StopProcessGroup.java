package io.kestra.plugin.nifi;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.Map;

/**
 * Task to schedule and stop all components within an Apache NiFi Process Group.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Stop an Apache NiFi Process Group",
    description = "Schedules all components within a specified NiFi Process Group to STOPPED state."
)
@Plugin(
    examples = {
        @Example(
            title = "Stop the root Process Group in NiFi",
            full = true,
            code = """
                id: nifi_stop_process_group
                namespace: company.team

                tasks:
                  - id: stop_root
                    type: io.kestra.plugin.nifi.StopProcessGroup
                    url: "https://localhost:8443"
                    username: "nifi-admin"
                    password: "{{ secret('NIFI_PASSWORD') }}"
                    sslVerify: false
                    processGroupId: "root"
                """
        )
    }
)
public class StopProcessGroup extends AbstractNifiConnection implements RunnableTask<StopProcessGroup.Output> {
    @NotNull
    @Schema(
        title = "The Process Group ID",
        description = "The unique identifier of the NiFi Process Group to stop. Can be a specific Process Group UUID or 'root' for the root group."
    )
    @PluginProperty
    private Property<String> processGroupId;

    @Override
    public StopProcessGroup.Output run(RunContext runContext) throws Exception {
        String token = this.authenticate(runContext);

        String renderedProcessGroupId = runContext.render(this.processGroupId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Process Group ID must be provided."));

        URI uri = this.resolveUri(runContext, "/flow/process-groups/" + renderedProcessGroupId);

        HttpRequest request = HttpRequest.builder()
            .uri(uri)
            .method("PUT")
            .body(HttpRequest.JsonRequestBody.builder()
                .content(Map.of(
                    "id", renderedProcessGroupId,
                    "state", "STOPPED"
                ))
                .build())
            .build();

        try (HttpClient client = this.createHttpClient(runContext, token)) {
            HttpResponse<String> response = client.request(request, String.class);
            runContext.logger().info("Successfully stopped NiFi Process Group '{}'", renderedProcessGroupId);

            return Output.builder()
                .processGroupId(renderedProcessGroupId)
                .build();
        }
    }

    /**
     * Output representing the result of stopping a NiFi Process Group.
     */
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The Process Group ID",
            description = "The unique identifier of the NiFi Process Group that was targeted."
        )
        private final String processGroupId;
    }
}
