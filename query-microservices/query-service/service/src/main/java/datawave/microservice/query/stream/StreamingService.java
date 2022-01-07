package datawave.microservice.query.stream;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.microservice.query.stream.runner.StreamingCall;
import datawave.microservice.querymetric.QueryMetricClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class StreamingService {
    private final QueryManagementService queryManagementService;
    private final QueryMetricClient queryMetricClient;
    
    private final ThreadPoolTaskExecutor streamingExecutor;
    
    public StreamingService(QueryManagementService queryManagementService, QueryMetricClient queryMetricClient, ThreadPoolTaskExecutor streamingExecutor) {
        this.queryManagementService = queryManagementService;
        this.queryMetricClient = queryMetricClient;
        this.streamingExecutor = streamingExecutor;
    }
    
    /**
     * Gets all pages of results for the given query and streams them to the configured listener.
     * <p>
     * Execute can only be called on a running query. <br>
     * Execute is a non-blocking call, and will return immediately. <br>
     * Only the query owner can call execute on the specified query.
     *
     * @param queryId
     *            the query id, not null
     * @param currentUser
     *            the user who called this method, not null
     * @param listener
     *            the listener which will handle the result pages, not null
     */
    public void execute(String queryId, ProxiedUserDetails currentUser, StreamingResponseListener listener) {
        // @formatter:off
        streamingExecutor.submit(
                new StreamingCall.Builder()
                        .setQueryManagementService(queryManagementService)
                        .setQueryMetricClient(queryMetricClient)
                        .setCurrentUser(currentUser)
                        .setQueryId(queryId)
                        .setListener(listener)
                        .build());
        // @formatter:on
    }
}
