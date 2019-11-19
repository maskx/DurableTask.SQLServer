# DurableTask.SQLServer

* No BolbStore
* Not support AbandonSessionAsync


## TODO:

### TaskSessionManager

* support database access retry when exception
* Implement AbandonSessionAsync Method
* AcceptSessionAsync 
  * need fetch timeout session -- **NEED TEST**
  * support receiveTimeout
  * support retry when no session retrieved


### PeekLockQueue

## Roadmap

### SQLServerOrchestrationService

### SQLServerInstanceStore

just copy from https://github.com/Azure/durabletask

**made some changes**

* GetOrchestrationStateAsync modified
  * check value is null
  * when OrchestrationState Table have no recorder, should check JumpStart table

### SQLServerBlobStore


### Issues

* maybe have a bug in ServiceBusOrchestrationService.WaitForOrchestrationAsync
* maybe have a bug in SqlServerInstanceStore.GetOrchestrationStateAsync
* UnitTest should run one by one, because there have static variable in orchestration define
* 