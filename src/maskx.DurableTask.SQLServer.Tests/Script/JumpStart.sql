CREATE TABLE [dbo].[DTF_JumpStart](
	[InstanceId] [nvarchar](50) NOT NULL,
	[ExecutionId] [nvarchar](50) NOT NULL,
	[SequenceNumber] [bigint] NOT NULL,
	[JumpStartTime] [datetime] NULL,
	[CompletedTime] [datetime] NULL,
	[CompressedSize] [bigint] NOT NULL,
	[CreatedTime] [datetime] NOT NULL,
	[Input] [nvarchar](max) NOT NULL,
	[LastUpdatedTime] [datetime] NOT NULL,
	[Name] [nvarchar](256) NOT NULL,
	[OrchestrationInstance] [nvarchar](max) NOT NULL,
	[OrchestrationStatus] [nvarchar](max) NOT NULL,
	[Output] [nvarchar](max) NULL,
	[ParentInstance] [nvarchar](max) NOT NULL,
	[Size] [bigint] NOT NULL,
	[Status] [nvarchar](50) NULL,
	[Tags] [nvarchar](max) NULL,
	[Version] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_DTF_JumpStart] PRIMARY KEY CLUSTERED 
(
	[InstanceId] ASC,
	[ExecutionId] ASC,
	[SequenceNumber] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO