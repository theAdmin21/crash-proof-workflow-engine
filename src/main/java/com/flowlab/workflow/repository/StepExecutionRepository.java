package com.flowlab.workflow.repository;

import com.flowlab.workflow.domain.StepExecution;
import com.flowlab.workflow.domain.StepStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface StepExecutionRepository extends JpaRepository<StepExecution, String> {

    List<StepExecution> findByExecutionIdOrderByStepIndexAsc(String executionId);

    Optional<StepExecution> findByExecutionIdAndStepIndex(String executionId, int stepIndex);

    // CRASH RECOVERY SCANNER QUERY:
    // Finds orphaned steps where worker crashed (heartbeat older than cutoff) or status is RETRYING
    @Query("SELECT s FROM StepExecution s WHERE s.status = :status AND s.lastHeartbeat < :cutoff")
    List<StepExecution> findStaleRunningSteps(@Param("status") StepStatus status, @Param("cutoff") Instant cutoff);
}
