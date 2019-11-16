CREATE TABLE [dbo].[DTF_TaskSession](
	[InstanceId] [nvarchar](50) NOT NULL,
	[LockedUntilUtc] [datetime2](7) NULL,
	[Status] [nvarchar](50) NULL,
	[SessionState] [nvarchar](max) NULL,
 CONSTRAINT [PK_DTF_TaskSession] PRIMARY KEY CLUSTERED 
(
	[InstanceId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO