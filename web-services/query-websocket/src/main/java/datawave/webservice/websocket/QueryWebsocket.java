package datawave.webservice.websocket;

import static datawave.security.util.SecurityConstants.REQUEST_LOGIN_TIME_HEADER;
import static datawave.security.websocket.WebsocketSecurityConfigurator.SESSION_SECURITY_IDENTITY;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.SecurityIdentity;

import datawave.security.websocket.WebsocketSecurityConfigurator;
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
 * <strong>NOTE: </strong> This uses the vendor-specific security extension {@link WebsocketSecurityConfigurator} to work around a websocket specification hole.
 * See <a href="https://github.com/jakartaee/websocket/issues/238">Jakarta EE #238</a> for more details.
 */
@ServerEndpoint(value = "/{logic-name}", encoders = {QueryResponseMessageJsonEncoder.class}, decoders = {JsonQueryMessageDecoder.class},
                configurator = WebsocketSecurityConfigurator.class)
public class QueryWebsocket {

    private static final String LOGIC_NAME = "logicName";
    private static final String ACTIVE_QUERY_FUTURE = "activeQueryFuture";
    private static final String ACTIVE_QUERY_ID = "activeQueryId";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private QueryExecutorBean queryExecutorBean;

    /**
     * Runs when a new websocket connection is opened. The logic name will be extracted from the URL path and stored into the session.
     *
     * @param logicName
     *            the logic name
     * @param session
     *            the session
     */
    @OnOpen
    public void openConnection(@PathParam("logic-name") String logicName, Session session) {
        session.getUserProperties().put(LOGIC_NAME, logicName);
    }

    /**
     * Runs when the websocket connection is closed. If a query is currently active for the session, it will be canceled.
     */
    @OnClose
    public void closeConnection(Session session) throws IOException {
        // Ensure the operation is executed using the permissions of the calling user.
        runAsSessionUser(session, () -> cancelActiveQuery(session));
    }

    /**
     * Runs when an incoming websocket message is received. The message is expected to be either a {@link CreateQueryMessage} or a {@link CancelMessage}.
     *
     * @param session
     *            the session
     * @param message
     *            the message
     */
    @OnMessage
    public void handleMessage(final Session session, QueryMessage message) throws IOException {
        switch (message.getType()) {
            case CREATE:
                // Ensure the operation is executed using the permissions of the calling user.
                runAsSessionUser(session, () -> createQuery(session, message));
                break;
            case CANCEL:
                // Ensure the operation is executed using the permissions of the calling user.
                runAsSessionUser(session, () -> cancelActiveQuery(session));
                break;
        }
    }

    /**
     * Executes the given runnable with the permissions of the calling user for the websocket session. We expected to find a {@link SecurityIdentity} stored in
     * the {@value WebsocketSecurityConfigurator#SESSION_SECURITY_IDENTITY} user property.
     *
     * @param session
     *            the session
     * @param runnable
     *            the operation to execute
     */
    private void runAsSessionUser(Session session, Runnable runnable) throws IOException {
        // Fetch the calling user's security identity from the session.
        Map<String,Object> userProperties = session.getUserProperties();
        final SecurityIdentity identity = (SecurityIdentity) userProperties.get(SESSION_SECURITY_IDENTITY);

        // If no identity was found, return an error message and close the session.
        if (identity == null) {
            if (log.isErrorEnabled()) {
                log.error("No SecurityIdentity found in session user property {}", SESSION_SECURITY_IDENTITY);
            }
            session.getAsyncRemote().sendObject(new QueryResponseMessage(ResponseType.ERROR, "Failed to load authenticated user"));
            session.close();
            return;
        }

        // Execute the runnable with the permissions of the calling user.
        identity.runAs(runnable);
    }

    /**
     * Creates a new active query for the session.
     *
     * @param session
     *            the session
     * @param message
     *            the message
     */
    private void createQuery(Session session, QueryMessage message) {
        // If the session already has an active query, do not allow another one to be created.
        if (session.getUserProperties().get(ACTIVE_QUERY_FUTURE) != null) {
            session.getAsyncRemote().sendObject(
                            new QueryResponseMessage(ResponseType.CREATION_FAILURE, "Query already active. Only one query per websocket is allowed."));
        } else {
            CreateQueryMessage cqm = (CreateQueryMessage) message;
            String logicName = (String) session.getUserProperties().get(LOGIC_NAME);
            QueryObserver observer = new QueryObserver(log, session);

            Long startTime = System.nanoTime();
            // Extract the login time from the session.
            Long loginTime = null;
            try {
                loginTime = Long.valueOf((String) session.getUserProperties().get(REQUEST_LOGIN_TIME_HEADER));
            } catch (Exception e) {
                // Ignore -- login time won't be available
            }

            // Create the query.
            Future<?> activeQuery = queryExecutorBean.executeAsync(logicName, cqm.getParameters(), startTime, loginTime, observer);
            // Add a property to track that there is now an active query associated with the session.
            session.getUserProperties().put(ACTIVE_QUERY_FUTURE, activeQuery);
        }
    }

    /**
     * Cancels any active query running in the session.
     *
     * @param session
     *            the session
     */
    private void cancelActiveQuery(Session session) {
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
                    log.warn("Failed to cancel query {}", activeQueryId, e);
                }
            }
        }
    }

    private static class QueryObserver implements AsyncQueryStatusObserver {
        private final Logger log;
        private final Session session;

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
                log.error("Unable to close peer connection after query {} completed.", queryId, e);
                throw new RuntimeException(e);
            }
        }
    }
}
