package datawave.query.tables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.remote.RemoteQueryLogic;
import datawave.core.query.remote.RemoteQueryService;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.config.RemoteQueryConfiguration;
import datawave.query.transformer.EventQueryTransformerSupport;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;

/**
 * <h1>Overview</h1> This is a query logic implementation that can handle delegating to a remote event query logic (i.e. one that returns an extension of
 * EventQueryResponseBase).
 */
public class RemoteEventQueryLogic extends BaseQueryLogic<EventBase> implements CheckpointableQueryLogic, RemoteQueryLogic<EventBase> {

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

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> auths) throws Exception {
        // @TODO: If this is a checkpointable query, then we may need to set the page size down to 1

        GenericResponse<String> createResponse = remoteQueryService.createQuery(getRemoteQueryLogic(), settings.toMap(), currentUser);
        setRemoteId(createResponse.getResult());
        return getConfig();
    }

    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {
        GenericResponse<String> planResponse = remoteQueryService.planQuery(getRemoteQueryLogic(), settings.toMap(), currentUser);
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
                remoteQueryService.close(getRemoteId(), currentUser);
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
        // @TODO Get an instanceof the underlying query logic?
        return new ShardQueryLogic().getOptionalQueryParameters();
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        // @TODO Get an instanceof the underlying query logic?
        return new ShardQueryLogic().getRequiredQueryParameters();
    }

    @Override
    public Set<String> getExampleQueries() {
        // @TODO Get an instanceof the underlying query logic?
        return new ShardQueryLogic().getExampleQueries();
    }

    public Query getSettings() {
        return getConfig().getQuery();
    }

    public void setSettings(Query settings) {
        getConfig().setQuery(settings);
    }

    /**
     * Implementations use the configuration to setup execution of a portion of their query. getTransformIterator should be used to get the partial results if
     * any.
     *
     * @param client
     *            The accumulo client
     * @param baseConfig
     *            The shard query configuration
     * @param checkpoint
     */
    @Override
    public void setupQuery(AccumuloClient client, GenericQueryConfiguration baseConfig, QueryCheckpoint checkpoint) throws Exception {
        RemoteQueryConfiguration config = (RemoteQueryConfiguration) baseConfig;
        setupQuery(config);
    }

    @Override
    public boolean isCheckpointable() {
        return getConfig().isCheckpointable();
    }

    @Override
    public void setCheckpointable(boolean checkpointable) {
        getConfig().setCheckpointable(checkpointable);
    }

    /**
     * This can be called at any point to get a checkpoint such that this query logic instance can be torn down to be rebuilt later. At a minimum this should be
     * called after the getTransformIterator is depleted of results.
     *
     * @param queryKey
     *            The query key to include in the checkpoint
     * @return The query checkpoint
     */
    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (!isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a query that is not checkpointable.  Try calling setCheckpointable(true) first.");
        }

        List<QueryCheckpoint> checkpoints = new ArrayList<>();

        // all we need is a placeholder. State is maintained on the remote side.
        checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(new QueryData())));

        return checkpoints;
    }

    @Override
    public QueryCheckpoint updateCheckpoint(QueryCheckpoint checkpoint) {
        // for the shard query logic, the query data objects automatically get update with
        // the last result returned, so the checkpoint should already be updated!
        return checkpoint;
    }

    private class RemoteQueryLogicIterator implements Iterator<EventBase> {
        private Queue<EventBase> data = new LinkedList<>();
        private boolean complete = false;

        @Override
        public boolean hasNext() {
            if (data.isEmpty() && !complete) {
                try {
                    EventQueryResponseBase response = (EventQueryResponseBase) remoteQueryService.next(getRemoteId(), currentUser);
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
