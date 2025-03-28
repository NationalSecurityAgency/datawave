package datawave.query.tables;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.remote.RemoteQueryService;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.config.RemoteQueryConfiguration;
import datawave.query.tables.remote.RemoteQueryLogic;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;

/**
 * <h1>Overview</h1> This is a base query logic implementation that can handle delegating to a remote query logic
 */
public abstract class BaseRemoteQueryLogic<T> extends BaseQueryLogic<T> implements RemoteQueryLogic<T> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(BaseRemoteQueryLogic.class);

    public static final String QUERY_ID = "queryId";

    protected RemoteQueryConfiguration config;

    protected RemoteQueryService remoteQueryService;

    protected UserOperations userOperations;

    protected QueryLogicTransformer<T,T> transformerInstance = null;

    /**
     * Basic constructor
     */
    public BaseRemoteQueryLogic() {
        super();
        if (log.isTraceEnabled())
            log.trace("Creating " + this.getClass().getSimpleName() + ": " + System.identityHashCode(this));
    }

    /**
     * Copy constructor
     *
     * @param other
     *            - another ShardQueryLogic object
     */
    public BaseRemoteQueryLogic(BaseRemoteQueryLogic other) {
        super(other);

        if (log.isTraceEnabled())
            log.trace("Creating Cloned " + this.getClass().getSimpleName() + ": " + System.identityHashCode(this) + " from " + System.identityHashCode(other));

        setRemoteQueryService(other.getRemoteQueryService());
        setUserOperations(other.getUserOperations());

        // Set ShardQueryConfiguration variables
        setConfig(RemoteQueryConfiguration.create(other.getConfig()));

        // transformer instance is created dynamically
    }

    public void setupConfig(GenericQueryConfiguration genericConfig) throws Exception {
        if (!RemoteQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a RemoteQueryConfiguration instance!!");
        }

        config = (RemoteQueryConfiguration) genericConfig;
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
        Map<String,List<String>> parms = settings.toMap();
        // we need to ensure that the remote query request includes the local query id for tracking purposes via query metrics.
        if (!parms.containsKey(QUERY_ID) && settings.getId() != null) {
            parms.put(QUERY_ID, Collections.singletonList(settings.getId().toString()));
        }
        GenericResponse<String> createResponse = remoteQueryService.createQuery(getRemoteQueryLogic(), parms, getCurrentUser());
        setRemoteId(createResponse.getResult());
        log.info("Local query " + settings.getId() + " maps to remote query " + getRemoteId());
        RemoteQueryConfiguration config = getConfig();
        config.setQuery(settings);
        return config;
    }

    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> auths, boolean expandFields, boolean expandValues) throws Exception {
        GenericResponse<String> planResponse = remoteQueryService.planQuery(getRemoteQueryLogic(), settings.toMap(), getCurrentUser());
        return planResponse.getResult();
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        // a transformer that turns EventBase objects into a response
        if (transformerInstance == null) {
            transformerInstance = createTransformer(settings, getMarkingFunctions(), getResponseObjectFactory());
        }

        return transformerInstance;
    }

    public abstract QueryLogicTransformer<T,T> createTransformer(Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory);

    @Override
    public void close() {

        super.close();

        log.debug("Closing " + this.getClass().getSimpleName() + ": " + System.identityHashCode(this));

        if (getRemoteId() != null) {
            try {
                remoteQueryService.close(getRemoteId(), getCurrentUser());
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
    public void setUserOperations(UserOperations userOperations) {
        this.userOperations = userOperations;
    }

    @Override
    public UserOperations getUserOperations() {
        return userOperations;
    }
}
