# DurableTask.SQLServer


* No BolbStore


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

### SQLServerBlobStore