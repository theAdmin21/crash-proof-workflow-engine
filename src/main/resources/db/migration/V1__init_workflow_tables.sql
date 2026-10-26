-- =========================================================================
-- FlowLab Crash-Proof Workflow Engine: Initial Schema
-- =========================================================================

CREATE TABLE IF NOT EXISTS workflows (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL REFERENCES workflows(id),
    status VARCHAR(32) NOT NULL,
    current_step_index INT NOT NULL DEFAULT 0,
    payload JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);

CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(64) PRIMARY KEY,
    execution_id VARCHAR(64) NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_name VARCHAR(128) NOT NULL,
    step_index INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    input_payload JSONB,
    output_payload JSONB,
    error_message TEXT,
    worker_id VARCHAR(128),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_step_executions_exec_id ON step_executions(execution_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_recovery ON step_executions(status, last_heartbeat);

-- Seed default Order Processing Workflow
INSERT INTO workflows (id, name, description)
VALUES (
    'order-processing-workflow',
    'Order Processing',
    'Process customer orders with payment, inventory, and notification steps.'
) ON CONFLICT (id) DO NOTHING;
