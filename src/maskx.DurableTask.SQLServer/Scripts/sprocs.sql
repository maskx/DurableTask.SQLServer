-- {0} is Schema name
-- {1} is Hub name

CREATE OR ALTER PROCEDURE {0}.{1}_CreateSession
    @NewInstanceEvents {1}_TaskEvent READONLY
AS
BEGIN
    DECLARE @existingStatus varchar(30) = (
        SELECT TOP 1 existing.RuntimeStatus
        FROM [{1}_Session] existing WITH (HOLDLOCK)
        INNER JOIN @NewInstanceEvents new ON new.InstanceID = existing.ID
    )

    -- CONSIDER: Allow overwriting if the instance is not active (i.e. dedupe status preferences)
    IF @existingStatus IS NOT NULL
        THROW 50001, N'An instance with this ID already exists.', 1;

    INSERT INTO [{1}_Session] (
        ID,
        ExecutionID,
        [Name],
        RuntimeStatus,
        InputText
    )
    SELECT
        InstanceID,
        ExecutionID,
        [Name],
        ISNULL(RuntimeStatus, 'Pending'),
        PayloadText
    FROM @NewInstanceEvents

    INSERT INTO {1}_NewEvent (
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        TaskID,
        [Name],
        PayloadText
    )
    SELECT
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        TaskID,
        [Name],
        PayloadText
    FROM @NewInstanceEvents
END
GO

CREATE OR ALTER PROCEDURE {0}.{1}_QuerySingleOrchestration
    @InstanceID nvarchar(100),
    @ExecutionID nvarchar(100) = NULL,
    @FetchInput bit = 1,
    @FetchOutput bit = 1
AS
BEGIN
    SELECT TOP 1
        ID AS InstanceID,
        ExecutionID,
        [Name],
        CreatedTime,
        LastUpdatedTime,
        CompletedTime,
        RuntimeStatus,
        CustomStatusText,
        CASE WHEN @FetchInput = 1 THEN InputText ELSE NULL END AS InputText,
        CASE WHEN @FetchOutput = 1 THEN OutputText ELSE NULL END AS OutputText
    FROM [{1}_Session]
    WHERE
        ID = @InstanceID AND
        (@ExecutionID IS NULL OR @ExecutionID = ExecutionID)
END
GO


CREATE OR ALTER PROCEDURE {0}.{1}_LockNextOrchestration
    @BatchSize int,
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()
    DECLARE @instanceID nvarchar(100)

    BEGIN TRANSACTION
    /* Lock order
       1. Instances (U)
       2. NewEvents (S)
    */

    -- Lock the first active instance that has pending messages.
    -- Delayed events from durable timers will have a non-null VisibleTime value.
    -- Non-active instances will never have their messages or history read.
    UPDATE TOP (1) [{1}_Session] WITH (READPAST)
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        @instanceID = ID
    FROM 
        [{1}_Session] I INNER JOIN {1}_NewEvent E WITH (READPAST) ON E.InstanceID = I.ID
    WHERE
        I.RuntimeStatus IN ('Pending', 'Running') AND
	    (I.LockExpiration IS NULL OR I.LockExpiration < @now) AND
        (E.VisibleTime IS NULL OR E.VisibleTime < @now)

    -- Result #1: The list of new events to fetch.
    -- IMPORTANT: DO NOT CHANGE THE ORDER OF RETURNED COLUMNS!
    -- TODO: Update the dequeue count
    SELECT TOP (@BatchSize)
        SequenceNumber,
        Timestamp,
        VisibleTime,
        DequeueCount,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText,
        DATEDIFF(millisecond, [Timestamp], @now) AS WaitTime
    FROM {1}_NewEvent --WITH (READPAST) -- This hint is to avoid deadlocking with CompleteTask
    WHERE InstanceID = @instanceID

    -- Bail if no events are returned - this implies that another thread already took them (???)
    IF @@ROWCOUNT = 0
    BEGIN
        ROLLBACK TRANSACTION
        RETURN
    END
     -- Result #2: The full event history for the locked instance
    SELECT *
    FROM {1}_HistoryEvent
    WHERE InstanceID = @instanceID

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE {0}.{1}_CheckpointOrchestration
    @NewOrchestrationEvents {1}_TaskEvent READONLY,
    @NewHistoryEvents {1}_TaskEvent READONLY,
    @NewTaskEvents {1}_TaskEvent READONLY,
    @UpdatedInstanceStatus {1}_TaskEvent READONLY,
    @DeletedControlMessages {1}_TaskEvent READONLY
AS
BEGIN
    BEGIN TRANSACTION
    /* Lock order
       1. Instances (X)
       2. NewEvents (X) -- existing first, then new
       3. Instances (S) -- FK constraint validation on NewEvents -> Instances
       4. History   (X)
       5. NewTasks  (X)
    */
    UPDATE [{1}_Session]
    SET
        ExecutionID = new.ExecutionID,
        CustomStatusText = new.CustomStatusText,
        RuntimeStatus = new.RuntimeStatus,
        OutputText = new.PayloadText,
        LastUpdatedTime = SYSUTCDATETIME(),
        CompletedTime = new.CompletedTime,
        LockExpiration = new.LockExpiration
    FROM
        [{1}_Session] existing
        INNER JOIN @UpdatedInstanceStatus new ON new.InstanceID = existing.ID

    IF @@ROWCOUNT = 0
        THROW 50000, 'The instance does not exist.', 1;

    INSERT INTO {1}_NewEvent (
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText
    ) 
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        RuntimeStatus,
        TaskID,
        Reason,
        PayloadText
    FROM @NewOrchestrationEvents

    
    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE new
    OUTPUT DELETED.InstanceID, DELETED.SequenceNumber
    FROM {1}_NewEvent new
        INNER JOIN @DeletedControlMessages M ON M.InstanceID = new.InstanceID AND M.SequenceNumber = new.SequenceNumber

    INSERT INTO {1}_HistoryEvent (
        InstanceID,
        SequenceNumber,
        ExecutionID,
        EventType,
        TaskID,
        IsPlayed,
        [Name],
        PayloadText,
        RuntimeStatus,
        VisibleTime)
    SELECT
        InstanceID,
        SequenceNumber,
        ExecutionID,
        EventType,
        TaskID,
        IsPlayed,
        [Name],
        PayloadText,
        RuntimeStatus,
        VisibleTime
    FROM @NewHistoryEvents

    -- TaskScheduled events
    INSERT INTO {1}_NewTask (
        -- Metadata columns
        VisibleTime,
        -- Orchestration columns
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText,
        LockedBy,
        LockExpiration
    )
    OUTPUT
        inserted.SequenceNumber,
        inserted.TaskID
    SELECT 
        VisibleTime,
        InstanceID,
        ExecutionID,
        EventType,
        [Name],
        TaskID,
        PayloadText,
        LockedBy,
        LockExpiration
    FROM @NewTaskEvents

    COMMIT TRANSACTION
END
GO


CREATE OR ALTER PROCEDURE {0}.{1}_LockNextTask
    @LockedBy nvarchar(100),
    @LockExpiration datetime2
AS
BEGIN
    DECLARE @now datetime2 = SYSUTCDATETIME()

    -- Update (lock) and return a single row.
    -- The PK_NewTasks_SequenceNumber hint is specified to help ensure in-order selection.
    -- TODO: Filter out tasks for instances that are in a non-running state (terminated, suspended, etc.)
    UPDATE TOP (1) {1}_NewTask WITH (READPAST)
    SET
        LockedBy = @LockedBy,
	    LockExpiration = @LockExpiration,
        DequeueCount = DequeueCount + 1
    OUTPUT
        INSERTED.*,
        DATEDIFF(millisecond, INSERTED.[Timestamp], @now) AS WaitTime
    FROM
        {1}_NewTask WITH (INDEX (PK_{0}_{1}_NewTask_SequenceNumber))
    WHERE
	    (LockExpiration IS NULL OR LockExpiration < @now) AND
        (VisibleTime IS NULL OR VisibleTime < @now)
END
GO


CREATE OR ALTER PROCEDURE {0}.{1}_CompleteTasks
    @CompletedTasks {1}_TaskEvent READONLY,
    @Results {1}_TaskEvent READONLY
AS
BEGIN
    BEGIN TRANSACTION
    /* Lock order:
       1. Instances (S)
       2. NewEvents (X)
       3. NewTasks  (X)
     */
    INSERT INTO {1}_NewEvent (
        R.VisibleTime,
        R.InstanceID,
        R.ExecutionID,
        R.EventType,
        R.[Name],
        R.TaskID,
        R.PayloadText
    ) 
    SELECT 
        R.VisibleTime,
        R.InstanceID,
        R.ExecutionID,
        R.EventType,
        R.[Name],
        R.TaskID,
        R.PayloadText
    FROM @Results R
        INNER JOIN [{1}_Session] I ON I.ID = R.InstanceID AND I.ExecutionID = R.ExecutionID

    -- We return the list of deleted messages so that the caller can issue a 
    -- warning about missing messages
    DELETE NT
    OUTPUT DELETED.SequenceNumber
    FROM {1}_NewTask NT INNER JOIN @CompletedTasks CT ON CT.SequenceNumber = NT.SequenceNumber

    COMMIT TRANSACTION
END
GO
