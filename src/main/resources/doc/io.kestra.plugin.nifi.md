This plugin provides tasks and triggers to interact with [Apache NiFi](https://nifi.apache.org/), enabling data orchestration workflows to monitor, start, stop, and trigger based on Apache NiFi components.

## Tasks

- `GetProcessGroupStatus`: Retrieves aggregate status metrics for a specified Process Group (or `root`), including queued FlowFiles count, queued bytes, and active thread count.
- `StartProcessGroup`: Starts and schedules all components within a specified Process Group to `RUNNING` state.
- `StopProcessGroup`: Stops all components within a specified Process Group to `STOPPED` state.

## Triggers

- `Trigger`: Periodically polls the Apache NiFi Bulletin Board and triggers workflow executions when new bulletins matching a configured severity level (`DEBUG`, `INFO`, `WARNING`, or `ERROR`, with `WARN` accepted as an alias for `WARNING`) are detected. Watermark tracking is maintained via Kestra's KV store. Note that NiFi retains at most 5 bulletins per component for up to 5 minutes, so higher polling intervals may miss bulletins under heavy activity.

## Authentication

Authentication is handled via Apache NiFi's REST API `/nifi-api/access/token` endpoint using username and password credentials to obtain a JWT Bearer token for subsequent requests. SSL certificate verification can be toggled using `sslVerify`.
