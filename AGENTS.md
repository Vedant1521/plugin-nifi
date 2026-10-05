# Kestra Apache NiFi Plugin

## What

- Provides plugin components under `io.kestra.plugin.nifi`.
- Includes classes such as `GetProcessGroupStatus`, `StartProcessGroup`, `StopProcessGroup`, `Trigger`.

## Why

- What user problem does this solve? Orchestrating data movement and lifecycle states across Apache NiFi Process Groups and reacting to Bulletin Board errors directly from Kestra workflows.
- Why would a team adopt this plugin in a workflow? It enables native, declarative NiFi management without writing external shell scripts or manual HTTP calls.
- What operational/business outcome does it enable? Reduces downtime, automates error detection and recovery in data pipelines, and integrates NiFi into the wider modern data stack.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `nifi`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.nifi.GetProcessGroupStatus`
- `io.kestra.plugin.nifi.StartProcessGroup`
- `io.kestra.plugin.nifi.StopProcessGroup`
- `io.kestra.plugin.nifi.Trigger`

### Project Structure

```
plugin-nifi/
├── src/main/java/io/kestra/plugin/nifi/
├── src/test/java/io/kestra/plugin/nifi/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
