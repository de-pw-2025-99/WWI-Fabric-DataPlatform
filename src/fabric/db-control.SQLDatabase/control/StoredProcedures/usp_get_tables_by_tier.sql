
CREATE PROCEDURE control.usp_get_tables_by_tier
    @load_tier INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        config_id,
        source_schema,
        source_table,
        source_query,
        target_lakehouse,
        target_schema,
        target_table,
        load_type,
        load_tier,
        watermark_column,
        watermark_data_type,
        ISNULL(last_watermark, '1900-01-01') AS last_watermark
    FROM control.load_config
    WHERE load_tier = @load_tier
      AND is_active = 1
    ORDER BY load_priority, source_schema, source_table;
END

GO

