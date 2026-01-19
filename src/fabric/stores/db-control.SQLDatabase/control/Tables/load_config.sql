CREATE TABLE [control].[load_config] (
    [config_id]           INT            IDENTITY (1, 1) NOT NULL,
    [source_type]         VARCHAR (128)  DEFAULT ('on-prem') NOT NULL,
    [source_db]           VARCHAR (128)  DEFAULT ('localhost') NOT NULL,
    [source_schema]       VARCHAR (128)  NOT NULL,
    [source_table]        VARCHAR (128)  NOT NULL,
    [source_query]        NVARCHAR (MAX) NULL,
    [target_lakehouse]    VARCHAR (128)  DEFAULT ('lh_bronze') NOT NULL,
    [target_schema]       VARCHAR (128)  DEFAULT ('dbo') NOT NULL,
    [target_table]        VARCHAR (128)  NOT NULL,
    [load_type]           VARCHAR (20)   NOT NULL,
    [load_tier]           INT            DEFAULT ((1)) NOT NULL,
    [load_priority]       INT            DEFAULT ((100)) NOT NULL,
    [watermark_column]    VARCHAR (128)  NULL,
    [watermark_data_type] VARCHAR (50)   NULL,
    [last_watermark]      VARCHAR (50)   NULL,
    [is_active]           BIT            DEFAULT ((1)) NOT NULL,
    [is_enabled_for_full] BIT            DEFAULT ((1)) NOT NULL,
    [row_count_source]    BIGINT         NULL,
    [row_count_loaded]    BIGINT         NULL,
    [last_load_status]    VARCHAR (20)   NULL,
    [last_load_start]     DATETIME2 (7)  NULL,
    [last_load_end]       DATETIME2 (7)  NULL,
    [last_error_message]  NVARCHAR (MAX) NULL,
    [created_date]        DATETIME2 (7)  DEFAULT (getutcdate()) NOT NULL,
    [created_by]          VARCHAR (128)  DEFAULT (suser_sname()) NOT NULL,
    [modified_date]       DATETIME2 (7)  DEFAULT (getutcdate()) NOT NULL,
    [modified_by]         VARCHAR (128)  DEFAULT (suser_sname()) NOT NULL,
    PRIMARY KEY CLUSTERED ([config_id] ASC),
    CONSTRAINT [CK_load_tier] CHECK ([load_tier]>=(1) AND [load_tier]<=(5)),
    CONSTRAINT [CK_load_type] CHECK ([load_type]='incremental' OR [load_type]='full'),
    CONSTRAINT [UQ_load_config_source] UNIQUE NONCLUSTERED ([source_schema] ASC, [source_table] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_load_config_tier_active]
    ON [control].[load_config]([load_tier] ASC, [is_active] ASC)
    INCLUDE([source_schema], [source_table], [watermark_column], [last_watermark], [load_type]);


GO

