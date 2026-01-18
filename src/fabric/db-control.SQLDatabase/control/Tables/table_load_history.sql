CREATE TABLE [control].[table_load_history] (
    [load_history_id]    INT            IDENTITY (1, 1) NOT NULL,
    [batch_run_id]       INT            NOT NULL,
    [config_id]          INT            NOT NULL,
    [pipeline_run_id]    VARCHAR (50)   NULL,
    [activity_run_id]    VARCHAR (50)   NULL,
    [start_time]         DATETIME2 (7)  DEFAULT (getutcdate()) NOT NULL,
    [end_time]           DATETIME2 (7)  NULL,
    [duration_seconds]   AS             (datediff(second,[start_time],[end_time])),
    [rows_read]          BIGINT         NULL,
    [rows_written]       BIGINT         NULL,
    [data_read_bytes]    BIGINT         NULL,
    [data_written_bytes] BIGINT         NULL,
    [watermark_start]    VARCHAR (50)   NULL,
    [watermark_end]      VARCHAR (50)   NULL,
    [status]             VARCHAR (20)   DEFAULT ('Running') NOT NULL,
    [error_message]      NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([load_history_id] ASC),
    CONSTRAINT [CK_table_load_status] CHECK ([status]='Skipped' OR [status]='Failed' OR [status]='Succeeded' OR [status]='Running'),
    CONSTRAINT [FK_table_load_batch] FOREIGN KEY ([batch_run_id]) REFERENCES [control].[batch_run_history] ([batch_run_id]),
    CONSTRAINT [FK_table_load_config] FOREIGN KEY ([config_id]) REFERENCES [control].[load_config] ([config_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_table_load_history_batch]
    ON [control].[table_load_history]([batch_run_id] ASC, [config_id] ASC);


GO

