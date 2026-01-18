
CREATE PROCEDURE control.usp_complete_batch_run
    @batch_run_id INT,
    @status VARCHAR(20),
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Calculate statistics
    DECLARE @tables_total INT, @tables_succeeded INT, @tables_failed INT, @total_rows BIGINT;
    
    SELECT 
        @tables_total = COUNT(*),
        @tables_succeeded = SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END),
        @tables_failed = SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END),
        @total_rows = SUM(ISNULL(rows_written, 0))
    FROM control.table_load_history
    WHERE batch_run_id = @batch_run_id;
    
    -- Update batch run
    UPDATE control.batch_run_history
    SET status = @status,
        end_time = GETUTCDATE(),
        tables_total = @tables_total,
        tables_succeeded = @tables_succeeded,
        tables_failed = @tables_failed,
        total_rows_loaded = @total_rows,
        error_message = @error_message
    WHERE batch_run_id = @batch_run_id;
    
    -- If successful, update watermarks in load_config
    IF @status = 'Succeeded'
    BEGIN
        UPDATE lc
        SET lc.last_watermark = tlh.watermark_end,
            lc.last_load_status = 'Success',
            lc.last_load_end = tlh.end_time,
            lc.row_count_loaded = tlh.rows_written,
            lc.modified_date = GETUTCDATE()
        FROM control.load_config lc
        INNER JOIN control.table_load_history tlh ON lc.config_id = tlh.config_id
        WHERE tlh.batch_run_id = @batch_run_id
          AND tlh.status = 'Succeeded';
    END
END

GO

