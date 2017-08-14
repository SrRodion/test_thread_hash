CREATE TABLE [TEST_FH].[Files] (
    [FР_ID]        INT            IDENTITY (1, 1) NOT NULL,
    [FileNameFull] NVARCHAR (300) NOT NULL,
    [FileHash]     NVARCHAR (50)  NULL,
    [FileErrors]   NVARCHAR (300) NULL,
    CONSTRAINT [PK_Files_FileID] PRIMARY KEY CLUSTERED ([FР_ID] ASC)
);

