package datawave.microservice.audit.replay.runner;

import java.util.concurrent.Future;

import datawave.microservice.audit.replay.status.Status;

/**
 * This provides access the status and worker task for an audit replay which is currently running.
 */
public class RunningReplay {
    
    final private Status status;
    final private Future future;
    
    public RunningReplay(Status status, Future future) {
        this.status = status;
        this.future = future;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public Future getFuture() {
        return future;
    }
}
