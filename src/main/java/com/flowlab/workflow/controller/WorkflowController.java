package com.flowlab.workflow.controller;

import com.flowlab.workflow.domain.StepExecution;
import com.flowlab.workflow.domain.WorkflowExecution;
import com.flowlab.workflow.engine.WorkflowEngineService;
import com.flowlab.workflow.repository.StepExecutionRepository;
import com.flowlab.workflow.repository.WorkflowExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workflows")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class WorkflowController {

    private final WorkflowEngineService engineService;
    private final WorkflowExecutionRepository executionRepository;
    private final StepExecutionRepository stepRepository;

    @PostMapping("/{workflowId}/start")
    public ResponseEntity<WorkflowExecution> startWorkflow(
            @PathVariable String workflowId,
            @RequestBody(required = false) Map<String, Object> payload) {
        String payloadJson = payload != null ? payload.toString() : "{\"orderId\": \"ORD-9921\"}";
        WorkflowExecution execution = engineService.startWorkflow(workflowId, payloadJson);
        return ResponseEntity.ok(execution);
    }

    @GetMapping("/executions/{executionId}")
    public ResponseEntity<WorkflowExecution> getExecution(@PathVariable String executionId) {
        return executionRepository.findById(executionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/executions/{executionId}/steps")
    public ResponseEntity<List<StepExecution>> getExecutionSteps(@PathVariable String executionId) {
        List<StepExecution> steps = stepRepository.findByExecutionIdOrderByStepIndexAsc(executionId);
        return ResponseEntity.ok(steps);
    }

    @PostMapping("/executions/{executionId}/resume")
    public ResponseEntity<Map<String, String>> resumeExecution(@PathVariable String executionId) {
        engineService.resumeFromLastFailure(executionId);
        return ResponseEntity.ok(Map.of("message", "Resumed execution from checkpoint", "executionId", executionId));
    }

    @GetMapping("/executions")
    public ResponseEntity<List<WorkflowExecution>> getAllExecutions() {
        return ResponseEntity.ok(executionRepository.findAll());
    }
}
