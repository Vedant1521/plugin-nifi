package io.kestra.plugin.nifi;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.BearerAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.SslOptions;
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
import java.util.Map;

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
        return createHttpClient(runContext, null);
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
        Boolean verifySsl = runContext.render(this.sslVerify).as(Boolean.class).orElse(true);

        HttpConfiguration.HttpConfigurationBuilder configurationBuilder = HttpConfiguration.builder();

        if (Boolean.FALSE.equals(verifySsl)) {
            configurationBuilder.ssl(
                SslOptions.builder()
                    .insecureTrustAllCertificates(Property.ofValue(true))
                    .build()
            );
        }

        if (bearerToken != null && !bearerToken.isBlank()) {
            configurationBuilder.auth(
                BearerAuthConfiguration.builder()
                    .token(Property.ofValue(bearerToken))
                    .build()
            );
        }

        return HttpClient.builder()
            .runContext(runContext)
            .configuration(configurationBuilder.build())
            .build();
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
        String baseUrl = runContext.render(this.url).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("NiFi URL must be configured."));

        String cleanBaseUrl = baseUrl.replaceAll("/+$", "");
        String cleanPath = path.startsWith("/") ? path : "/" + path;

        if (!cleanBaseUrl.endsWith("/nifi-api") && !cleanPath.startsWith("/nifi-api")) {
            cleanBaseUrl = cleanBaseUrl + "/nifi-api";
        }

        return URI.create(cleanBaseUrl + cleanPath);
    }

    /**
     * Fetches a JWT token from Apache NiFi using username and password via POST /access/token.
     *
     * @param runContext the current run context
     * @return the JWT token string
     * @throws Exception if credentials are missing, evaluation fails, or the request fails
     */
    protected String fetchJwtToken(RunContext runContext) throws Exception {
        String renderedUsername = runContext.render(this.username).as(String.class).orElse(null);
        String renderedPassword = runContext.render(this.password).as(String.class).orElse(null);

        if (renderedUsername == null || renderedUsername.isBlank() || renderedPassword == null || renderedPassword.isBlank()) {
            throw new IllegalArgumentException("NiFi authentication failed: both username and password must be configured to fetch a JWT token.");
        }

        URI tokenUri = resolveUri(runContext, "/access/token");

        HttpRequest request = HttpRequest.builder()
            .uri(tokenUri)
            .method("POST")
            .body(HttpRequest.UrlEncodedRequestBody.of(Map.of(
                "username", renderedUsername,
                "password", renderedPassword
            )))
            .build();

        try (HttpClient client = createHttpClient(runContext)) {
            HttpResponse<String> response = client.request(request, String.class);
            String token = response.getBody();
            if (token == null || token.isBlank()) {
                throw new IllegalStateException("Received an empty JWT token from NiFi endpoint: " + tokenUri);
            }
            return token;
        }
    }

    /**
     * Alias for {@link #fetchJwtToken(RunContext)}.
     *
     * @param runContext the current run context
     * @return the JWT token string
     * @throws Exception if authentication fails
     */
    protected String authenticate(RunContext runContext) throws Exception {
        return fetchJwtToken(runContext);
    }
}
