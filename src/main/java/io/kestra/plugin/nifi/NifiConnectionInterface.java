package io.kestra.plugin.nifi;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Interface defining connection properties for Apache NiFi.
 */
public interface NifiConnectionInterface {
    @Schema(
        title = "The Apache NiFi base URL",
        description = "The fully qualified URL pointing to your Apache NiFi cluster or instance (e.g., https://localhost:8443 or http://localhost:8080)."
    )
    Property<String> getUrl();

    @Schema(
        title = "The username for NiFi authentication"
    )
    Property<String> getUsername();

    @Schema(
        title = "The password for NiFi authentication"
    )
    @PluginProperty(secret = true)
    Property<String> getPassword();

    @Schema(
        title = "Whether to verify SSL certificates",
        description = "Set to false to disable SSL verification (e.g., for self-signed certificates)."
    )
    Property<Boolean> getSslVerify();

    @Schema(
        title = "Client certificate for mutual TLS (mTLS) authentication",
        description = "The client certificate content or certificate file path used for mutual TLS authentication."
    )
    @PluginProperty(secret = true)
    Property<String> getClientCertificate();
}
