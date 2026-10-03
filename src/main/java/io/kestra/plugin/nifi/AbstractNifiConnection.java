package io.kestra.plugin.nifi;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
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

/**
 * Base abstract task providing connection, SSL configuration, and authentication capabilities for Apache NiFi.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractNifiConnection extends Task implements NifiConnectionInterface {
    @NotNull
    @Schema(
        title = "The Apache NiFi base URL",
        description = "The fully qualified URL pointing to your Apache NiFi cluster or instance (e.g., https://localhost:8443 or http://localhost:8080)."
    )
    protected Property<String> url;

    @Schema(
        title = "The username for NiFi authentication"
    )
    protected Property<String> username;

    @Schema(
        title = "The password for NiFi authentication"
    )
    @PluginProperty(secret = true)
    @ToString.Exclude
    protected Property<String> password;

    @Schema(
        title = "Whether to verify SSL certificates",
        description = "Set to false to disable SSL verification (e.g., for self-signed certificates)."
    )
    @Builder.Default
    protected Property<Boolean> sslVerify = Property.ofValue(true);

    @Schema(
        title = "Client certificate for mutual TLS (mTLS) authentication",
        description = "The client certificate content or certificate file path used for mutual TLS authentication."
    )
    @PluginProperty(secret = true)
    @ToString.Exclude
    protected Property<String> clientCertificate;

    /**
     * Initializes and returns Kestra's internal HTTP client configured with NiFi connection and SSL settings.
     *
     * @param runContext the current run context
     * @return configured HttpClient
     * @throws IllegalVariableEvaluationException if variable rendering fails
     */
    protected HttpClient createHttpClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return NifiService.createHttpClient(this, runContext);
    }

    /**
     * Initializes and returns Kestra's internal HTTP client configured with NiFi connection, SSL settings,
     * and an optional Bearer JWT token.
     *
     * @param runContext the current run context
     * @param bearerToken optional Bearer token string
     * @return configured HttpClient
     * @throws IllegalVariableEvaluationException if variable rendering fails
     */
    protected HttpClient createHttpClient(RunContext runContext, String bearerToken) throws IllegalVariableEvaluationException {
        return NifiService.createHttpClient(this, runContext, bearerToken);
    }

    /**
     * Resolves an API endpoint path against the NiFi base URL, ensuring the '/nifi-api' root prefix is correctly applied.
     *
     * @param runContext the current run context
     * @param path endpoint path (e.g., "/access/token" or "process-groups/root")
     * @return resolved URI
     * @throws IllegalVariableEvaluationException if URL rendering fails
     */
    protected URI resolveUri(RunContext runContext, String path) throws IllegalVariableEvaluationException {
        return NifiService.resolveUri(this, runContext, path);
    }

    /**
     * Fetches a JWT token from Apache NiFi using username and password via POST /access/token.
     *
     * @param runContext the current run context
     * @return the JWT token string
     * @throws Exception if credentials are missing, evaluation fails, or the request fails
     */
    protected String fetchJwtToken(RunContext runContext) throws Exception {
        return NifiService.authenticate(this, runContext);
    }

    /**
     * Alias for {@link #fetchJwtToken(RunContext)}.
     *
     * @param runContext the current run context
     * @return the JWT token string
     * @throws Exception if authentication fails
     */
    protected String authenticate(RunContext runContext) throws Exception {
        return NifiService.authenticate(this, runContext);
    }
}
