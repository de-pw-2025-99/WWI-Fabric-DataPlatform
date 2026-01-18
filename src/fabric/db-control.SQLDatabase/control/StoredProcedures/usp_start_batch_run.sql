
CREATE PROCEDURE control.usp_start_batch_run
    @batch_cutoff_time DATETIME2,
    @pipeline_run_id VARCHAR(50) = NULL,
    @batch_run_id INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO control.batch_run_history (batch_cutoff_time, pipeline_run_id, status)
    VALUES (@batch_cutoff_time, @pipeline_run_id, 'Running');
    
    SET @batch_run_id = SCOPE_IDENTITY();
    
    SELECT @batch_run_id AS batch_run_id;
END

GO

