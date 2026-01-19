CREATE TABLE [control].[batch_run_history] (
    [batch_run_id]      INT            IDENTITY (1, 1) NOT NULL,
    [batch_cutoff_time] DATETIME2 (7)  NOT NULL,
    [pipeline_run_id]   VARCHAR (50)   NULL,
    [status]            VARCHAR (20)   DEFAULT ('Running') NOT NULL,
    [start_time]        DATETIME2 (7)  DEFAULT (getutcdate()) NOT NULL,
    [end_time]          DATETIME2 (7)  NULL,
    [tables_total]      INT            NULL,
    [tables_succeeded]  INT            NULL,
    [tables_failed]     INT            NULL,
    [total_rows_loaded] BIGINT         NULL,
    [error_message]     NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([batch_run_id] ASC),
    CONSTRAINT [CK_batch_status] CHECK ([status]='Cancelled' OR [status]='Failed' OR [status]='Succeeded' OR [status]='Running')
);


GO

