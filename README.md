# DurableTask.SQLServer

* No BolbStore support
* No JumpStart support

## TODO

* how to support abandon session and message should be investigated
* Tracing & log support
* add more unit test
* using stored proc instead of  SQL string

### TaskSessionManager

* support database access retry when exception
* Implement AbandonSessionAsync Method
* AcceptSessionAsync 
  * need fetch timeout session -- **NEED TEST**
  * support receiveTimeout
  * support retry when no session retrieved


## SQLServerInstanceStore

just copy from https://github.com/Azure/durabletask

**made some changes**

* GetOrchestrationStateAsync modified
  * check value is null
  * when OrchestrationState Table have no recorder, should check JumpStart table

### SQLServerBlobStore

Not supported

### Issues

* maybe have a bug in ServiceBusOrchestrationService.WaitForOrchestrationAsync
* maybe have a bug in SqlServerInstanceStore.GetOrchestrationStateAsync
* UnitTest should run one by one, because there have static variable in orchestration define
* 