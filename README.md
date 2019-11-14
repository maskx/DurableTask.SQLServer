# DurableTask.SQLServer


* No BolbStore
* Not support context.CreateTimer

## TODO:

### TaskSessionManager

* support database access retry when exception
* Implement AbandonSessionAsync Method
* AcceptSessionAsync 
  * need fetch timeout session -- **NEED TEST**
  * support receiveTimeout
  * support retry when no session retrieved


### PeekLockQueue

* save message in database


## Roadmap

### SQLServerOrchestrationService

### SQLServerInstanceStore

### SQLServerBlobStore