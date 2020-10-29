-- {0} is Schema name
-- {1} is Hub name

CREATE OR ALTER PROCEDURE {0}.{1}_CreateSession
    @InstanceID 
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

    INSERT INTO [{0}.{1}_Session] (
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

    INSERT INTO [{0}.{1}_NewEvent] (
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
