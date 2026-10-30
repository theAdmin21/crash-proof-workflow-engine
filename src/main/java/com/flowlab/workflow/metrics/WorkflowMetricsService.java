package com.flowlab.workflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class WorkflowMetricsService {

    private final MeterRegistry meterRegistry;

    public void recordWorkflowStarted(String workflowId) {
        meterRegistry.counter("workflow_executions_total", "workflow", workflowId, "status", "STARTED").increment();
    }

    public void recordWorkflowCompleted(String workflowId) {
        meterRegistry.counter("workflow_executions_total", "workflow", workflowId, "status", "COMPLETED").increment();
    }

    public void recordWorkflowFailed(String workflowId) {
        meterRegistry.counter("workflow_executions_total", "workflow", workflowId, "status", "FAILED").increment();
    }

    public void recordWorkflowCompensated(String workflowId) {
        meterRegistry.counter("workflow_executions_total", "workflow", workflowId, "status", "COMPENSATED").increment();
    }

    public void recordStepDuration(String stepName, String status, long durationMs) {
        Timer.builder("workflow_step_duration_seconds")
                .tag("step", stepName)
                .tag("status", status)
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordStepRetry(String stepName) {
        meterRegistry.counter("workflow_step_retries_total", "step", stepName).increment();
    }
}
