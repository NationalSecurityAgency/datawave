package datawave.query.tables;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions;
import datawave.query.config.RemoteQueryConfiguration;
import datawave.query.tables.remote.RemoteQueryLogic;
import datawave.query.transformer.EventQueryTransformerSupport;
import datawave.security.authorization.UserOperations;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.common.remote.RemoteQueryService;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;

/**
 * <h1>Overview</h1> This is a query logic implementation that can handle delegating to a remote event query logic (i.e. one that returns an extension of
 * EventQueryResponseBase).
 */
public class RemoteEventQueryLogic extends BaseQueryLogic<EventBase> implements RemoteQueryLogic<EventBase> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(RemoteEventQueryLogic.class);

    private RemoteQueryConfiguration config;

    private RemoteQueryService remoteQueryService;

    private UserOperations userOperations;

    private QueryLogicTransformer transformerInstance = null;

    /**
     * Basic constructor
     */
    public RemoteEventQueryLogic() {
        super();
        if (log.isTraceEnabled())
            log.trace("Creating RemoteQueryLogic: " + System.identityHashCode(this));
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another ShardQueryLogic object
     */
    public RemoteEventQueryLogic(RemoteEventQueryLogic other) {
        super(other);

        if (log.isTraceEnabled())
            log.trace("Creating Cloned RemoteQueryLogic: " + System.identityHashCode(this) + " from " + System.identityHashCode(other));

        setRemoteQueryService(other.getRemoteQueryService());

        // Set ShardQueryConfiguration variables
        setConfig(RemoteQueryConfiguration.create(other));
    }

    public String getRemoteId() {
        return getConfig().getRemoteId();
    }

    public void setRemoteId(String id) {
        getConfig().setRemoteId(id);
        getConfig().setQueryString("( metrics = '" + remoteQueryService.getQueryMetricsURI(id).toString() + "' )");
    }

    public String getRemoteQueryLogic() {
        return getConfig().getRemoteQueryLogic();
    }

    public void setRemoteQueryLogic(String remoteQueryLogic) {
        getConfig().setRemoteQueryLogic(remoteQueryLogic);
    }

    public Object getCallerObject() {
        return getPrincipal();
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> auths) throws Exception {
        GenericResponse<String> createResponse = remoteQueryService.createQuery(getRemoteQueryLogic(), settings.toMap(), getCallerObject());
        setRemoteId(createResponse.getResult());
        return getConfig();
    }

    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {
        GenericResponse<String> planResponse = remoteQueryService.planQuery(getRemoteQueryLogic(), settings.toMap(), getCallerObject());
        return planResponse.getResult();
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!RemoteQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a RemoteQueryConfiguration instance!!");
        }

        config = (RemoteQueryConfiguration) genericConfig;

        // Create an iterator that returns a stream of EventBase objects
        iterator = new RemoteQueryLogicIterator();
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        // a transformer that turns EventBase objects into a response
        if (transformerInstance == null) {
            transformerInstance = new EventBaseTransformer(settings, getMarkingFunctions(), getResponseObjectFactory());
        }

        return transformerInstance;
    }

    @Override
    public RemoteEventQueryLogic clone() {
        return new RemoteEventQueryLogic(this);
    }

    @Override
    public void close() {

        super.close();

        log.debug("Closing RemoteQueryLogic: " + System.identityHashCode(this));

        if (getRemoteId() != null) {
            try {
                remoteQueryService.close(getRemoteId(), getCallerObject());
            } catch (Exception e) {
                log.error("Failed to close remote query", e);
            }
        }
    }

    @Override
    public RemoteQueryConfiguration getConfig() {
        if (config == null) {
            config = RemoteQueryConfiguration.create();
        }

        return config;
    }

    public void setConfig(RemoteQueryConfiguration config) {
        this.config = config;
    }

    public RemoteQueryService getRemoteQueryService() {
        return remoteQueryService;
    }

    public void setRemoteQueryService(RemoteQueryService remoteQueryService) {
        this.remoteQueryService = remoteQueryService;
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return new ShardQueryLogic().getOptionalQueryParameters();
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return new ShardQueryLogic().getRequiredQueryParameters();
    }

    @Override
    public Set<String> getExampleQueries() {
        return new ShardQueryLogic().getExampleQueries();
    }

    public Query getSettings() {
        return getConfig().getQuery();
    }

    public void setSettings(Query settings) {
        getConfig().setQuery(settings);
    }

    private class RemoteQueryLogicIterator implements Iterator<EventBase> {
        private Queue<EventBase> data = new LinkedList<>();
        private boolean complete = false;

        @Override
        public boolean hasNext() {
            if (data.isEmpty() && !complete) {
                try {
                    EventQueryResponseBase response = (EventQueryResponseBase) remoteQueryService.next(getRemoteId(), getCallerObject());
                    if (response != null) {
                        if (response.getReturnedEvents() == 0) {
                            if (response.isPartialResults()) {
                                EventBase e = responseObjectFactory.getEvent();
                                e.setIntermediateResult(true);
                                data.add(e);
                            } else {
                                complete = true;
                            }
                        } else {
                            for (EventBase event : response.getEvents()) {
                                data.add(event);
                            }
                        }
                    } else {
                        // in this case we must have gotten a 204, so we are done
                        complete = true;
                    }
                } catch (Exception e) {
                    complete = true;
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
            return !data.isEmpty();
        }

        @Override
        public EventBase next() {
            return data.poll();
        }
    }

    private class EventBaseTransformer extends EventQueryTransformerSupport<EventBase,EventBase> {

        public EventBaseTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
            super("notable", settings, markingFunctions, responseObjectFactory);
        }

        public EventBaseTransformer(BaseQueryLogic<Map.Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                        ResponseObjectFactory responseObjectFactory) {
            super(logic, settings, markingFunctions, responseObjectFactory);
        }

        @Override
        public EventBase transform(EventBase input) throws EmptyObjectException {
            return input;
        }

    }

    @Override
    public void setUserOperations(UserOperations userOperations) {
        this.userOperations = userOperations;
    }

    @Override
    public UserOperations getUserOperations() {
        return userOperations;
    }
}
