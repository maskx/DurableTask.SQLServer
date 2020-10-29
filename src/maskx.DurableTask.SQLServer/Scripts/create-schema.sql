-- {0} is Schema name
-- {1} is Hub name

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{0}')
    EXEC('CREATE SCHEMA {0}');

-- Create custom types
-- WARNING: Reordering fields is a breaking change!
IF TYPE_ID(N'{0}.{1}_TaskEvent') IS NULL
    CREATE TYPE [{0}].[{1}_TaskEvent] AS TABLE (
        SequenceNumber bigint NULL,
        VisibleTime datetime2 NULL,
        InstanceID nvarchar(100) NULL,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NULL,
        [Name] nvarchar(256) NULL,
        RuntimeStatus varchar(30) NULL,
        TaskID int NULL,
        Reason nvarchar(max) NULL,
        PayloadText nvarchar(max) NULL,
        CustomStatusText nvarchar(max) NULL,
        IsPlayed bit NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
        CompletedTime datetime2 NULL
    )

-- Create tables

IF OBJECT_ID(N'{0}.{1}_Session', 'U') IS NULL
BEGIN
	CREATE TABLE [{0}].[{1}_Session] (
		ID nvarchar(100) NOT NULL CONSTRAINT PK_{0}_{1}_Session PRIMARY KEY,
		ExecutionID nvarchar(100) NOT NULL,
        [Name] nvarchar(256) NOT NULL,
		CreatedTime datetime2 NOT NULL CONSTRAINT {0}_{1}_Session_CreatedTime DEFAULT SYSUTCDATETIME(),
		LastUpdatedTime datetime2 NULL,
        CompletedTime datetime2 NULL,
		RuntimeStatus varchar(20) NOT NULL INDEX IX_{0}_{1}_Session_RuntimeStatus NONCLUSTERED,
		CustomStatusText nvarchar(max) NULL,
		InputText nvarchar(max) NULL,
		OutputText nvarchar(max) NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
	)

    -- TODO: Indexes to improve search performance
END

IF OBJECT_ID(N'{0}.{1}_NewEvent', 'U') IS NULL
BEGIN
    CREATE TABLE [{0}].[{1}_NewEvent] (
        -- Metadata columns
        SequenceNumber bigint IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT {0}_{1}_NewEvent_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_NewEvent_DequeueCount DEFAULT 0,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_{0}_{1}_NewEvent_Session_ID FOREIGN KEY REFERENCES {0}.{1}_Session(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        RuntimeStatus varchar(30) NULL,
        TaskID int NULL,
        Reason nvarchar(max) NULL,
        PayloadText nvarchar(max) NULL,
    )

    CREATE CLUSTERED INDEX IX_{0}_{1}_NewEvent_InstanceId_SequenceNumber ON {0}.{1}_NewEvent (InstanceID, SequenceNumber)
END

IF OBJECT_ID(N'{0}.{1}_NewTask', 'U') IS NULL
    CREATE TABLE [{0}].[{1}_NewTask] (
        -- Metadata columns
        SequenceNumber bigint CONSTRAINT PK_{0}_{1}NewTask_SequenceNumber PRIMARY KEY IDENTITY NOT NULL,
        [Timestamp] datetime2 NOT NULL CONSTRAINT DF_{0}_{1}_NewTask_Timestamp DEFAULT SYSUTCDATETIME(),
        VisibleTime datetime2 NULL,
        DequeueCount int NOT NULL CONSTRAINT DF_{0}_{1}_NewTask_DequeueCount DEFAULT 0,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
        -- Orchestration columns
        InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_{0}_{1}_NewTask_Session_ID FOREIGN KEY REFERENCES {0}.{1}_Session(ID) ON DELETE CASCADE,
        ExecutionID nvarchar(100) NULL,
        EventType varchar(30) NOT NULL,
        [Name] nvarchar(256) NULL,
        TaskID int NOT NULL,
        PayloadText nvarchar(max) NULL,
    )

IF OBJECT_ID(N'{0}.{1}_History', 'U') IS NULL
    CREATE TABLE {0}.{1}_History (
	    ID nvarchar(100) NOT NULL CONSTRAINT PK_{0}_{1}_History PRIMARY KEY,
		ExecutionID nvarchar(100) NOT NULL,
        [Name] nvarchar(256) NOT NULL,
		CreatedTime datetime2 NOT NULL CONSTRAINT {0}_{1}_History_CreatedTime DEFAULT SYSUTCDATETIME(),
		LastUpdatedTime datetime2 NULL,
        CompletedTime datetime2 NULL,
		RuntimeStatus varchar(20) NOT NULL INDEX IX_{0}_{1}_History_RuntimeStatus NONCLUSTERED,
		CustomStatusText nvarchar(max) NULL,
		InputText nvarchar(max) NULL,
		OutputText nvarchar(max) NULL,
        LockedBy nvarchar(100) NULL,
        LockExpiration datetime2 NULL,
    )


IF OBJECT_ID(N'{0}.{1}_HistoryEvent', 'U') IS NULL
    CREATE TABLE {0}.{1}_HistoryEvent (
	    InstanceID nvarchar(100) NOT NULL CONSTRAINT FK_{0}_{1}_HistoryEvent_History_ID FOREIGN KEY REFERENCES {0}.{1}_History(ID) ON DELETE CASCADE,
	    ExecutionID nvarchar(100) NOT NULL,
        SequenceNumber bigint NOT NULL,
	    EventType varchar(30) NOT NULL,
	    TaskID int NULL,
	    IsPlayed bit NOT NULL CONSTRAINT {0}_{1}_HistoryEvent_IsPlayed DEFAULT 0,
	    [Timestamp] datetime2 NOT NULL CONSTRAINT {0}_{1}_HistoryEvent_Timestamp DEFAULT SYSUTCDATETIME(),
	    [Name] nvarchar(256) NULL,
	    PayloadText nvarchar(max) NULL,
	    RuntimeStatus varchar(20) NULL,
        VisibleTime datetime2 NULL,
        CONSTRAINT PK_{0}_{1}_HistoryEvent_InstanceID_ExecutionID_SequenceNumber PRIMARY KEY (InstanceID, ExecutionID, SequenceNumber)
    )

