
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