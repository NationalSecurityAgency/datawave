package datawave.microservice.query.stream;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.microservice.query.stream.runner.StreamingCall;
import datawave.microservice.querymetric.QueryMetricClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

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
    
    public void execute(String queryId, MultiValueMap<String,String> parameters, ProxiedUserDetails currentUser, StreamingResponseListener listener) {
        // @formatter:off
        streamingExecutor.submit(
                new StreamingCall.Builder()
                        .setQueryManagementService(queryManagementService)
                        .setQueryMetricClient(queryMetricClient)
                        .setParameters(parameters)
                        .setCurrentUser(currentUser)
                        .setQueryId(queryId)
                        .setListener(listener)
                        .build());
        // @formatter:on
    }
}
