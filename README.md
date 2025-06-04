# vest-vertx
Spike for creating a VEST component using Vertx


## Stage 1:

-  Process can receive an event object into the processor
  -  It can handle where the objectId is repeated
    - where version already exists it will skip the processing
    - where version does not exist it will add the event to the history and process the event

- Processing will send all active events to the transform channel
- Processing will only publish events when the event version is new, only one higher than
    then last processed version and also the last processed version should be in state PUBLISHED

## Stage 2:
Introduction of persistence
- To some extent this is working.  The consumer has been put in charge of only allowing one transaction to be 
processed at a time for any given objectId.  This is a form of pessimistic locking but ensures that the
complexity of the processing is kept simple.  While the event loop ensures that only one message is processed
at any one time on the event loop thread, it does not guarantee that work is not being done in parallel
(e.g. async persistence activity) in a worker thread pool.
- The ordering is not quite as per the current system which doesn't allow anything other than the storage
of the event to be done until the previous event (if not version 1) has been fully processed.  In this case
we will forward any received message to the transformer channel even if it's not the actual next required
version.  This might cause minor delays should messages come in out of order but it will speed up the 
response times for the next event in the sequence to be processed when its time comes as the transform work
will already have been completed.

Other things to think about:

- The consumer will remove messages that it has once they are published.  This a) probably needs
changing to only being removed when the event is fully processed and b) we need to think about how we handle
situations where say version 1 was processed then the application restarted and version 2 was received. At
the moment it will have no history of version 1 in the map.  Sounds like it needs to look for previous versions
if it doesn't find one in the map.
- Out of order test is not working.  The consumer logic needs tweaking to not send any message in to the processor
while there is an event already out for processing.

Things to add:

- xslt transform
- xsd validation
- error handling to UEH flow
- ack handling
- sending messages to the broker
- using UniLock to help with transactionality and locking