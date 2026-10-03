package io.kestra.plugin.nifi;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.BearerAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.SslOptions;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import java.net.URI;
import java.util.Map;

/**
 * Service providing reusable HTTP client creation, URI resolution, and authentication for Apache NiFi.
 */
public abstract class NifiService {

    /**
     * Initializes and returns Kestra's internal HTTP client configured with NiFi connection and SSL settings.
     *
     * @param connection the NiFi connection configuration
     * @param runContext the current run context
     * @return configured HttpClient
     * @throws IllegalVariableEvaluationException if variable rendering fails
     */
    public static HttpClient createHttpClient(NifiConnectionInterface connection, RunContext runContext) throws IllegalVariableEvaluationException {
        return createHttpClient(connection, runContext, null);
    }

    /**
     * Initializes and returns Kestra's internal HTTP client configured with NiFi connection, SSL settings,
     * and an optional Bearer JWT token.
     *
     * @param connection the NiFi connection configuration
     * @param runContext the current run context
     * @param bearerToken optional Bearer token string
     * @return configured HttpClient
     * @throws IllegalVariableEvaluationException if variable rendering fails
     */
    public static HttpClient createHttpClient(NifiConnectionInterface connection, RunContext runContext, String bearerToken) throws IllegalVariableEvaluationException {
        Boolean verifySsl = runContext.render(connection.getSslVerify()).as(Boolean.class).orElse(true);

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
     * @param connection the NiFi connection configuration
     * @param runContext the current run context
     * @param path endpoint path (e.g., "/access/token" or "/flow/bulletin-board")
     * @return resolved URI
     * @throws IllegalVariableEvaluationException if URL rendering fails
     */
    public static URI resolveUri(NifiConnectionInterface connection, RunContext runContext, String path) throws IllegalVariableEvaluationException {
        String baseUrl = runContext.render(connection.getUrl()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("NiFi URL must be configured."));

        String cleanBaseUrl = baseUrl.replaceAll("/+$", "");
        String cleanPath = path.startsWith("/") ? path : "/" + path;

        if (!cleanBaseUrl.endsWith("/nifi-api") && !cleanPath.startsWith("/nifi-api")) {
            cleanBaseUrl = cleanBaseUrl + "/nifi-api";
        }

        return URI.create(cleanBaseUrl + cleanPath);
    }

    /**
     * Authenticates with NiFi using username and password via POST /access/token, returning the JWT token.
     *
     * @param connection the NiFi connection configuration
     * @param runContext the current run context
     * @return the JWT token string
     * @throws Exception if credentials are missing, evaluation fails, or the request fails
     */
    public static String authenticate(NifiConnectionInterface connection, RunContext runContext) throws Exception {
        String renderedUsername = runContext.render(connection.getUsername()).as(String.class).orElse(null);
        String renderedPassword = runContext.render(connection.getPassword()).as(String.class).orElse(null);

        if (renderedUsername == null || renderedUsername.isBlank() || renderedPassword == null || renderedPassword.isBlank()) {
            throw new IllegalArgumentException("NiFi authentication failed: both username and password must be configured to fetch a JWT token.");
        }

        URI tokenUri = resolveUri(connection, runContext, "/access/token");

        HttpRequest request = HttpRequest.builder()
            .uri(tokenUri)
            .method("POST")
            .body(HttpRequest.UrlEncodedRequestBody.of(Map.of(
                "username", renderedUsername,
                "password", renderedPassword
            )))
            .build();

        try (HttpClient client = createHttpClient(connection, runContext)) {
            HttpResponse<String> response = client.request(request, String.class);
            String token = response.getBody();
            if (token == null || token.isBlank()) {
                throw new IllegalStateException("Received an empty JWT token from NiFi endpoint: " + tokenUri);
            }
            return token;
        }
    }
}
