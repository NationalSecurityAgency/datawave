package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;

import java.util.concurrent.Callable;

public class FindWorkTask implements Callable<Void> {
    protected final QueryStorageCache cache;
    protected final QueryExecutor executor;
    
    public FindWorkTask(QueryStorageCache cache, QueryExecutor executor) {
        this.cache = cache;
        this.executor = executor;
    }
    
    @Override
    public Void call() throws Exception {
        for (QueryStatus queryStatus : cache.getQueryStatus()) {
            switch (queryStatus.getQueryState()) {
                case CLOSED:
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.CLOSE, false);
                    break;
                case CANCELED:
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.CANCEL, false);
                    break;
                case DEFINED:
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.CREATE, false);
                    // could also have plan tasks
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.PLAN, false);
                    break;
                case CREATED:
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.NEXT, false);
                    // could also have plan tasks
                    executor.handleRequest(queryStatus.getQueryKey().getQueryId(), QueryRequest.Method.PLAN, false);
                    break;
                case FAILED:
                    // noop
                    break;
            }
        }
        return null;
    }
}
