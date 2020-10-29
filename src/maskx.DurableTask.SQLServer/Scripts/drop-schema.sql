-- {0} is Schema name
-- {1} is Hub name

-- Drop stored procedures
DROP PROCEDURE IF EXISTS [{0}].[{1}_LockNextOrchestration]
DROP PROCEDURE IF EXISTS [{0}].[{1}_CheckpointOrchestration]
DROP PROCEDURE IF EXISTS [{0}].[{1}_CreateSession]
DROP PROCEDURE IF EXISTS [{0}].[{1}_QuerySingleOrchestration]
DROP PROCEDURE IF EXISTS [{0}].[{1}_CompleteTasks]
DROP PROCEDURE IF EXISTS [{0}].[{1}_LockNextTask]


-- Tables
DROP TABLE IF EXISTS [{0}].[{1}_NewEvent]
DROP TABLE IF EXISTS [{0}].[{1}_NewTask]
DROP TABLE IF EXISTS [{0}].[{1}_Session]

-- Custom types
DROP TYPE IF EXISTS [{0}].[{1}_TaskEvent]


