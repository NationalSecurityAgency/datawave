package datawave.webservice.websocket;

import static datawave.webservice.metrics.Constants.REQUEST_LOGIN_TIME_HEADER;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.security.websocket.WebsocketSecurityConfigurator;
import datawave.security.websocket.WebsocketSecurityInterceptor;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.AsyncQueryStatusObserver;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.websocket.codec.JsonQueryMessageDecoder;
import datawave.webservice.websocket.codec.QueryResponseMessageJsonEncoder;
import datawave.webservice.websocket.messages.CancelMessage;
import datawave.webservice.websocket.messages.CreateQueryMessage;
import datawave.webservice.websocket.messages.QueryMessage;
import datawave.webservice.websocket.messages.QueryResponseMessage;
import datawave.webservice.websocket.messages.QueryResponseMessage.ResponseType;

/**
 * A websocket-based interface for running DATAWAVE queries. The websocket lifespan is a single query. A client connects to this endpoint and submits a query
 * request using a {@link CreateQueryMessage}. The query is executed asynchronously and a created message followed by each page of results is sent back to the
 * client over the websocket. When the query is finished, a completion message is sent and then websocket is closed. Additionally, the client may send a
 * {@link CancelMessage} while the query is in progress in order to cancel execution of the query. Note that if there is a problem creating the query, a
 * creation failure message is sent and the websocket is closed.
 * <p>
 * Per the JSR-356 specification (section 2.1.1), since we have not configured the endpoint otherwise, there shall be one instance of this class per endpoint,
 * per peer.
 * <p>
 * <strong>NOTE: </strong> This uses vendor-specific security extensions to work around a websocket specification hole. See
 * <a href="https://java.net/jira/browse/WEBSOCKET_SPEC-238">WEBSOCKET_SPEC-238</a> for more details.
 */
@ServerEndpoint(value = "/{logic-name}", encoders = {QueryResponseMessageJsonEncoder.class}, decoders = {JsonQueryMessageDecoder.class},
                configurator = WebsocketSecurityConfigurator.class // required to propagate security along to individual websocket notification calls
)
@Interceptors({WebsocketSecurityInterceptor.class})
// required to propagate security along to individual websocket notification calls
public class QueryWebsocket {
    private static final String LOGIC_NAME = "logicName";
    private static final String ACTIVE_QUERY_FUTURE = "activeQueryFuture";
    private static final String ACTIVE_QUERY_ID = "activeQueryId";

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private QueryExecutorBean queryExecutorBean;

    @OnOpen
    public void openConnection(@PathParam("logic-name") String logicName, Session session) throws IOException {
        session.getUserProperties().put(LOGIC_NAME, logicName);
    }

    @OnClose
    public void closeConnection(Session session) throws IOException {
        cancelActiveQuery(session);
    }

    @OnMessage
    public void handleMessage(final Session session, QueryMessage message) {
        switch (message.getType()) {
            case CREATE: {
                if (session.getUserProperties().get(ACTIVE_QUERY_FUTURE) != null) {
                    session.getAsyncRemote().sendObject(
                                    new QueryResponseMessage(ResponseType.CREATION_FAILURE, "Query already active. Only one query per websocket is allowed."));
                } else {
                    CreateQueryMessage cqm = (CreateQueryMessage) message;
                    String logicName = (String) session.getUserProperties().get(LOGIC_NAME);
                    QueryObserver observer = new QueryObserver(log, session);

                    Long startTime = System.nanoTime();
                    Long loginTime = null;
                    try {
                        loginTime = Long.valueOf((String) session.getUserProperties().get(REQUEST_LOGIN_TIME_HEADER));
                    } catch (Exception e) {
                        // Ignore -- login time won't be available
                    }

                    Future<?> activeQuery = queryExecutorBean.executeAsync(logicName, cqm.getParameters(), startTime, loginTime, observer);
                    session.getUserProperties().put(ACTIVE_QUERY_FUTURE, activeQuery);
                }
            }
                break;
            case CANCEL: {
                cancelActiveQuery(session);
            }
                break;
        }
    }

    protected void cancelActiveQuery(Session session) {
        Future<?> activeQuery = (Future<?>) session.getUserProperties().get(ACTIVE_QUERY_FUTURE);
        if (activeQuery != null && !activeQuery.isDone()) {
            // Attempt to cancel the async query call. This will cause the async call to return when it is between next calls.
            activeQuery.cancel(true);
            // Attempt to cancel the actual query. This should cancel an active next call.
            String activeQueryId = (String) session.getUserProperties().get(ACTIVE_QUERY_ID);
            if (activeQueryId != null) {
                try {
                    queryExecutorBean.cancel(activeQueryId);
                } catch (Exception e) {
                    log.warn("Failed to cancel query " + activeQueryId, e);
                }
            }
        }
    }

    private static class QueryObserver implements AsyncQueryStatusObserver {
        private Logger log;
        private Session session;

        public QueryObserver(Logger log, Session session) {
            this.log = log;
            this.session = session;
        }

        @Override
        public void queryCreated(GenericResponse<String> createQueryResponse) {
            session.getUserProperties().put(ACTIVE_QUERY_ID, createQueryResponse.getResult());
            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.CREATED, createQueryResponse.getResult()));
        }

        @Override
        public void queryResultsAvailable(BaseQueryResponse results) {
            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.RESULTS, results));
        }

        @Override
        public void queryCreateException(QueryException ex) {
            VoidResponse response = new VoidResponse();
            response.addException(ex);
            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.CREATION_FAILURE, "Query creation failed", response));
            try {
                session.close();
            } catch (IOException e) {
                log.error("Unable to close peer connection after query create failed.", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void queryException(QueryException ex) {
            session.getUserProperties().remove(ACTIVE_QUERY_ID);
            session.getUserProperties().remove(ACTIVE_QUERY_FUTURE);

            VoidResponse response = new VoidResponse();
            response.addException(ex);
            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.ERROR, response));
        }

        @Override
        public void queryFinished(String queryId) {
            session.getUserProperties().remove(ACTIVE_QUERY_ID);
            session.getUserProperties().remove(ACTIVE_QUERY_FUTURE);

            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.COMPLETED));
            try {
                session.close();
            } catch (IOException e) {
                log.error("Unable to close peer connection after query " + queryId + " completed.", e);
                throw new RuntimeException(e);
            }
        }
    }
}
