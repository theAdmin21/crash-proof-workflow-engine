ALTER TABLE workflow_executions ALTER COLUMN payload TYPE TEXT;
ALTER TABLE step_executions ALTER COLUMN input_payload TYPE TEXT;
ALTER TABLE step_executions ALTER COLUMN output_payload TYPE TEXT;
