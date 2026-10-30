package com.flowlab.workflow.repository;

import com.flowlab.workflow.domain.WorkflowExecution;
import com.flowlab.workflow.domain.WorkflowStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkflowExecutionRepository extends JpaRepository<WorkflowExecution, String> {
    List<WorkflowExecution> findByStatus(WorkflowStatus status);
}
