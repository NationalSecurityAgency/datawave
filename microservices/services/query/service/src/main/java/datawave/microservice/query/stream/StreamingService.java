package datawave.microservice.query.stream;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.microservice.query.stream.runner.StreamingCall;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;

@Service
public class StreamingService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final QueryManagementService queryManagementService;
    private final QueryMetricClient queryMetricClient;
    
    private final ThreadPoolTaskExecutor streamingCallExecutor;
    
    public StreamingService(QueryManagementService queryManagementService, QueryMetricClient queryMetricClient, ThreadPoolTaskExecutor streamingCallExecutor) {
        this.queryManagementService = queryManagementService;
        this.queryMetricClient = queryMetricClient;
        this.streamingCallExecutor = streamingCallExecutor;
    }
    
    /**
     * Creates a query using the given query logic and parameters, and streams all pages of results to the configured listener.
     * <p>
     * Created queries will start running immediately. <br>
     * Auditing is performed before the query is started. <br>
     * Stop a running query gracefully using {@link QueryManagementService#close} or forcefully using {@link QueryManagementService#cancel}. <br>
     * Stop, and restart a running query using {@link QueryManagementService#reset}. <br>
     * Create a copy of a running query using {@link QueryManagementService#duplicate}. <br>
     * Aside from a limited set of admin actions, only the query owner can act on a running query.
     *
     * @param queryLogicName
     *            the requested query logic, not null
     * @param parameters
     *            the query parameters, not null
     * @param pool
     *            the pool to target, may be null
     * @param currentUser
     *            the user who called this method, not null
     * @param listener
     *            the listener which will handle the result pages, not null
     * @return the query id
     * @throws BadRequestQueryException
     *             if parameter validation fails
     * @throws BadRequestQueryException
     *             if query logic parameter validation fails
     * @throws UnauthorizedQueryException
     *             if the user doesn't have access to the requested query logic
     * @throws BadRequestQueryException
     *             if security marking validation fails
     * @throws BadRequestQueryException
     *             if auditing fails
     * @throws QueryException
     *             if query storage fails
     * @throws NoResultsQueryException
     *             if no query results are found
     * @throws QueryException
     *             if there is an unknown error
     */
    public String createAndExecute(String queryLogicName, MultiValueMap<String,String> parameters, String pool, DatawaveUserDetails currentUser,
                    DatawaveUserDetails serverUser, StreamingResponseListener listener) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: {}/createAndExecute from {} with params: {}", queryLogicName, user, parameters);
        } else {
            log.info("Request: {}/createAndExecute from {}", queryLogicName, user);
        }
        
        String queryId = queryManagementService.create(queryLogicName, parameters, pool, currentUser).getResult();
        submitStreamingCall(queryId, currentUser, serverUser, listener);
        return queryId;
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
     * @param serverUser
     *            the server user, not null
     * @param listener
     *            the listener which will handle the result pages, not null
     */
    public void execute(String queryId, DatawaveUserDetails currentUser, DatawaveUserDetails serverUser, StreamingResponseListener listener) {
        log.info("Request: {}/execute from {}", queryId, ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        submitStreamingCall(queryId, currentUser, serverUser, listener);
    }
    
    private void submitStreamingCall(String queryId, DatawaveUserDetails currentUser, DatawaveUserDetails serverUser, StreamingResponseListener listener) {
        // @formatter:off
        streamingCallExecutor.submit(
                new StreamingCall.Builder()
                        .setQueryManagementService(queryManagementService)
                        .setQueryMetricClient(queryMetricClient)
                        .setQueryId(queryId)
                        .setCurrentUser(currentUser)
                        .setServerUser(serverUser)
                        .setListener(listener)
                        .build());
        // @formatter:on
    }
}
