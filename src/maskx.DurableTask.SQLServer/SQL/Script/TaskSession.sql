
CREATE TABLE [dbo].[DTF_TaskSession](
	[InstanceId] [nvarchar](50) NOT NULL,
	[LockedUntilUtc] [datetime2](7) NULL,
	[Status] [nvarchar](50) NULL,
	[SessionState] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
