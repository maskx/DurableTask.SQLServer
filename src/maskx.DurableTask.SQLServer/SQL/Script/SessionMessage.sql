CREATE TABLE [dbo].[DTF_SessionMessage](
	[InstanceId] [nvarchar](50) NOT NULL,
	[ExecutionId] [nvarchar](50) NOT NULL,
	[SequenceNumber] [bigint] NOT NULL,
	[OrchestrationInstance] [nvarchar](max) NULL,
	[LockedUntilUtc] [datetime2](7) NULL,
	[Status] [nvarchar](50) NULL,
	[Event] [nvarchar](max) NULL,
	[ExtensionData] [nvarchar](max) NULL,
 CONSTRAINT [PK_DTF_SessionMessage_1] PRIMARY KEY CLUSTERED 
(
	[InstanceId] ASC,
	[ExecutionId] ASC,
	[SequenceNumber] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO