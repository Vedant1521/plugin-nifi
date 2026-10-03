package io.kestra.plugin.nifi;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;

/**
 * Task to retrieve the status and aggregate metrics of an Apache NiFi Process Group.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get the status of an Apache NiFi Process Group",
    description = "Fetches the current aggregate status and metrics (queued FlowFiles, queued bytes, and active threads) of a specified NiFi Process Group."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch the aggregate status of the root Process Group in NiFi",
            full = true,
            code = """
                id: nifi_process_group_status
                namespace: company.team

                tasks:
                  - id: get_root_status
                    type: io.kestra.plugin.nifi.GetProcessGroupStatus
                    url: "https://localhost:8443"
                    username: "nifi-admin"
                    password: "{{ secret('NIFI_PASSWORD') }}"
                    sslVerify: false
                    processGroupId: "root"
                """
        )
    }
)
public class GetProcessGroupStatus extends AbstractNifiConnection implements RunnableTask<GetProcessGroupStatus.Output> {
    @NotNull
    @Schema(
        title = "The Process Group ID",
        description = "The unique identifier of the NiFi Process Group to retrieve status for. Can be a specific Process Group UUID or 'root' for the root group."
    )
    @PluginProperty
    private Property<String> processGroupId;

    @Override
    public GetProcessGroupStatus.Output run(RunContext runContext) throws Exception {
        String token = this.authenticate(runContext);

        String renderedProcessGroupId = runContext.render(this.processGroupId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Process Group ID must be provided."));

        URI statusUri = this.resolveUri(runContext, "/flow/process-groups/" + renderedProcessGroupId + "/status");

        HttpRequest request = HttpRequest.builder()
            .uri(statusUri)
            .method("GET")
            .build();

        try (HttpClient client = this.createHttpClient(runContext, token)) {
            HttpResponse<String> response = client.request(request, String.class);

            JsonNode rootNode = JacksonMapper.ofJson().readTree(response.getBody());
            JsonNode snapshot = rootNode.path("processGroupStatus").path("aggregateSnapshot");

            Integer queuedCount = snapshot.hasNonNull("queuedCount") ? snapshot.get("queuedCount").asInt() : null;
            Long queuedBytes = snapshot.hasNonNull("queuedBytes") ? snapshot.get("queuedBytes").asLong() : null;
            Integer activeThreadCount = snapshot.hasNonNull("activeThreadCount") ? snapshot.get("activeThreadCount").asInt() : null;

            runContext.logger().info("Process Group '{}' status: queuedCount={}, queuedBytes={}, activeThreadCount={}",
                renderedProcessGroupId, queuedCount, queuedBytes, activeThreadCount);

            return Output.builder()
                .queuedCount(queuedCount)
                .queuedBytes(queuedBytes)
                .activeThreadCount(activeThreadCount)
                .build();
        }
    }

    /**
     * Output representing the aggregate metrics of a NiFi Process Group.
     */
    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The number of FlowFiles queued in the Process Group",
            description = "Aggregate count of all FlowFiles currently queued across all queues within this Process Group."
        )
        private final Integer queuedCount;

        @Schema(
            title = "The total size of FlowFiles queued in the Process Group (in bytes)",
            description = "Aggregate byte size of all FlowFiles currently queued across all queues within this Process Group."
        )
        private final Long queuedBytes;

        @Schema(
            title = "The number of active threads in the Process Group",
            description = "Aggregate count of active threads executing tasks across all components in this Process Group."
        )
        private final Integer activeThreadCount;
    }
}
