package io.kestra.plugin.nifi;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class NifiTest {
    private static WireMockServer wireMockServer;
    private static String wireMockUrl;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(wireMockConfig()
            .dynamicPort()
            .containerThreads(10)
            .asynchronousResponseThreads(2));
        wireMockServer.start();
        wireMockUrl = "http://localhost:" + wireMockServer.port();
    }

    @AfterAll
    static void stopWireMock() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void setupStubs() {
        wireMockServer.resetAll();

        // POST /nifi-api/access/token
        wireMockServer.stubFor(post(urlEqualTo("/nifi-api/access/token"))
            .willReturn(aResponse()
                .withStatus(201)
                .withHeader("Content-Type", "text/plain")
                .withBody("fake-jwt-token")));

        // GET /nifi-api/flow/process-groups/root/status
        wireMockServer.stubFor(get(urlEqualTo("/nifi-api/flow/process-groups/root/status"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "processGroupStatus": {
                        "aggregateSnapshot": {
                          "flowFilesQueued": 5,
                          "bytesQueued": 1024,
                          "activeThreadCount": 2
                        }
                      }
                    }
                    """)));

        // PUT /nifi-api/flow/process-groups/root
        wireMockServer.stubFor(put(urlEqualTo("/nifi-api/flow/process-groups/root"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "id": "root",
                      "state": "RUNNING"
                    }
                    """)));

        // GET /nifi-api/flow/bulletin-board
        wireMockServer.stubFor(get(urlEqualTo("/nifi-api/flow/bulletin-board"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "bulletinBoard": {
                        "bulletins": [
                          {
                            "id": 1,
                            "groupId": "root",
                            "sourceId": "processor-1",
                            "canRead": true,
                            "bulletin": {
                              "id": 1,
                              "level": "ERROR",
                              "message": "NiFi mock error message",
                              "sourceName": "LogMessage"
                            }
                          }
                        ]
                      }
                    }
                    """)));
    }

    @Test
    void testGetProcessGroupStatus() throws Exception {
        GetProcessGroupStatus task = GetProcessGroupStatus.builder()
            .id("get_status")
            .type(GetProcessGroupStatus.class.getName())
            .url(Property.ofValue(wireMockUrl))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue("password"))
            .sslVerify(Property.ofValue(false))
            .processGroupId(Property.ofValue("root"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Collections.emptyMap());
        GetProcessGroupStatus.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getQueuedCount(), is(5));
        assertThat(output.getQueuedBytes(), is(1024L));
        assertThat(output.getActiveThreadCount(), is(2));
    }

    @Test
    void testStartProcessGroup() throws Exception {
        StartProcessGroup task = StartProcessGroup.builder()
            .id("start_pg")
            .type(StartProcessGroup.class.getName())
            .url(Property.ofValue(wireMockUrl))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue("password"))
            .sslVerify(Property.ofValue(false))
            .processGroupId(Property.ofValue("root"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Collections.emptyMap());
        StartProcessGroup.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getProcessGroupId(), is("root"));

        wireMockServer.verify(putRequestedFor(urlEqualTo("/nifi-api/flow/process-groups/root"))
            .withHeader("Authorization", equalTo("Bearer fake-jwt-token"))
            .withRequestBody(matchingJsonPath("$.state", equalTo("RUNNING"))));
    }

    @Test
    void testStopProcessGroup() throws Exception {
        StopProcessGroup task = StopProcessGroup.builder()
            .id("stop_pg")
            .type(StopProcessGroup.class.getName())
            .url(Property.ofValue(wireMockUrl))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue("password"))
            .sslVerify(Property.ofValue(false))
            .processGroupId(Property.ofValue("root"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Collections.emptyMap());
        StopProcessGroup.Output output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getProcessGroupId(), is("root"));

        wireMockServer.verify(putRequestedFor(urlEqualTo("/nifi-api/flow/process-groups/root"))
            .withHeader("Authorization", equalTo("Bearer fake-jwt-token"))
            .withRequestBody(matchingJsonPath("$.state", equalTo("STOPPED"))));
    }

    @Test
    void testTrigger() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("nifi_trigger_" + io.kestra.core.utils.IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockUrl))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue("password"))
            .sslVerify(Property.ofValue(false))
            .level(Property.ofValue("ERROR"))
            .build();

        var triggerEntry = TestsUtils.mockTrigger(runContextFactory, trigger);
        ConditionContext conditionContext = triggerEntry.getKey();
        TriggerContext triggerContext = triggerEntry.getValue();

        Optional<Execution> executionOptional = trigger.evaluate(conditionContext, triggerContext);

        assertThat(executionOptional.isPresent(), is(true));
        Execution execution = executionOptional.get();
        assertThat(execution.getTrigger(), notNullValue());
        assertThat(execution.getTrigger().getVariables(), notNullValue());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bulletins = (List<Map<String, Object>>) execution.getTrigger().getVariables().get("bulletins");
        assertThat(bulletins, notNullValue());
        assertThat(bulletins, hasSize(1));
        assertThat(bulletins.get(0).get("id"), is(1));
    }

    @Test
    void testTriggerWatermarkAndRestart() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("nifi_trigger_" + io.kestra.core.utils.IdUtils.create())
            .type(Trigger.class.getName())
            .url(Property.ofValue(wireMockUrl))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue("password"))
            .sslVerify(Property.ofValue(false))
            .level(Property.ofValue("ERROR"))
            .build();

        var triggerEntry = TestsUtils.mockTrigger(runContextFactory, trigger);
        ConditionContext conditionContext = triggerEntry.getKey();
        TriggerContext triggerContext = triggerEntry.getValue();

        // 1. First evaluation: bulletin id=1 present -> should trigger
        Optional<Execution> firstEval = trigger.evaluate(conditionContext, triggerContext);
        assertThat(firstEval.isPresent(), is(true));

        // 2. Second evaluation with identical bulletin board -> should not trigger again
        Optional<Execution> secondEval = trigger.evaluate(conditionContext, triggerContext);
        assertThat(secondEval.isPresent(), is(false));

        // 3. New bulletin id=2 arrives
        wireMockServer.stubFor(get(urlEqualTo("/nifi-api/flow/bulletin-board"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "bulletinBoard": {
                        "bulletins": [
                          {
                            "id": 1,
                            "bulletin": { "id": 1, "level": "ERROR", "message": "First error" }
                          },
                          {
                            "id": 2,
                            "bulletin": { "id": 2, "level": "ERROR", "message": "Second error" }
                          }
                        ]
                      }
                    }
                    """)));

        Optional<Execution> thirdEval = trigger.evaluate(conditionContext, triggerContext);
        assertThat(thirdEval.isPresent(), is(true));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bulletinsAfterNew = (List<Map<String, Object>>) thirdEval.get().getTrigger().getVariables().get("bulletins");
        assertThat(bulletinsAfterNew, hasSize(1));
        assertThat(bulletinsAfterNew.get(0).get("id"), is(2));

        // 4. NiFi service restart: in-memory counter resets, highest bulletin id is 1 (< stored watermark 2)
        wireMockServer.stubFor(get(urlEqualTo("/nifi-api/flow/bulletin-board"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("""
                    {
                      "bulletinBoard": {
                        "bulletins": [
                          {
                            "id": 1,
                            "bulletin": { "id": 1, "level": "ERROR", "message": "Error after restart" }
                          }
                        ]
                      }
                    }
                    """)));

        Optional<Execution> fourthEval = trigger.evaluate(conditionContext, triggerContext);
        assertThat(fourthEval.isPresent(), is(true));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bulletinsAfterRestart = (List<Map<String, Object>>) fourthEval.get().getTrigger().getVariables().get("bulletins");
        assertThat(bulletinsAfterRestart, hasSize(1));
        assertThat(bulletinsAfterRestart.get(0).get("id"), is(1));
    }
}
