CREATE TABLE [dbo].[DTF_WorkItem](
	[InstanceId] [nvarchar](50) NOT NULL,
	[ExecutionId] [nvarchar](50) NOT NULL,
	[SequenceNumber] [bigint] NOT NULL,
	[HistoryEvent] [nvarchar](max) NOT NULL,
	[EventTimestamp] [datetime] NOT NULL,
	[TimeStamp] [datetimeoffset](7) NOT NULL,
 CONSTRAINT [PK_DTF_WorkItem] PRIMARY KEY CLUSTERED 
(
	[InstanceId] ASC,
	[ExecutionId] ASC,
	[SequenceNumber] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO