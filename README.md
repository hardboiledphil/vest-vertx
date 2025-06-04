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

- Introduction of persistence