package datawave.webservice.annotation;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.ejb.EJBContext;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.data.v1.AnnotationReader;
import datawave.annotation.data.v1.FederatedAnnotationReader;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.ResponseRewriterContext;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;

/**
 * Per-request context for the {@link AnnotationManagerBean}, holding the state and resources needed to process annotation requests. This class also implements
 * {@link ResponseRewriterContext} to provide request-level information to response rewriters.
 */
public class AnnotationManagerRequestContext implements ResponseRewriterContext {

    private static final Logger log = LoggerFactory.getLogger(AnnotationManagerRequestContext.class);

    private final AnnotationManagerConfig config;
    private final AccumuloConnectionFactory connectionFactory;
    private final AccumuloConnectionRequestBean accumuloConnectionRequestBean;
    private final ResponseObjectFactory responseObjectFactory;
    private final ExecutorService annotationFederatedReadExecutor;

    /** the user performing this request */
    private final DatawavePrincipal datawavePrincipal;

    /** the dn of the user performing this request */
    private final String userDn;

    /** proxy servers involved in this request */
    private final Collection<String> proxyServers;

    /** authorizations pulled from the query parameters for this request */
    private final String queryAuths;

    /** the final set of merged query and user authorizations. */
    private Set<Authorizations> authorizations;

    /** the accumulo client to use for this request - obtained from the connection pool and must be returned. */
    private AccumuloClient client;

    /** used to lookup uuids and obtain internal identifiers, scoped to the caller's authorizations */
    private LookupUUIDService lookupUUIDService;

    /** used to _read_ annotations directly from accumulo, scoped to the caller's authorizations */
    private AnnotationReader annotationDataAccess;

    /** Cache lookups for unique analytic source hashes so we don't perform lookups more than once. TODO: make this a proper cross-request cache? */
    private final Map<String,Optional<AnnotationSource>> retrievedSourcesCache = new java.util.HashMap<>();

    /** Lookup an annotation source or retrieve it from the cache */
    public Optional<AnnotationSource> getAnnotationSource(String analyticHash) {
        return retrievedSourcesCache.computeIfAbsent(analyticHash, key -> annotationDataAccess.getAnnotationSource(analyticHash));
    }

    /**
     * Initialize the request context with the objects needed to perform various request state initialization. Validation of the objects provided is performed
     * in the various initialize methods exposed by this class. Each of the objects provided as parameters are expected to be shared across many requests.
     *
     * @param config
     *            the annotation manager configuration
     * @param ctx
     *            the ejb context - used for retrieving the principal for he qrequest
     * @param connectionFactory
     *            the accumulo connection factory - used for getting accumulo clients
     * @param accumuloConnectionRequestBean
     *            the accumulo connection request bean - used for tracking accumulo clients rerquests
     * @param responseObjectFactory
     *            the response object factory used for creating LookupUUID responses.
     * @param annotationFederatedReadExecutor
     *            the executor service used for federated reads
     */
    public AnnotationManagerRequestContext(AnnotationManagerConfig config, EJBContext ctx, AccumuloConnectionFactory connectionFactory,
                    AccumuloConnectionRequestBean accumuloConnectionRequestBean, ResponseObjectFactory responseObjectFactory,
                    ExecutorService annotationFederatedReadExecutor) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.accumuloConnectionRequestBean = accumuloConnectionRequestBean;
        this.responseObjectFactory = responseObjectFactory;
        this.annotationFederatedReadExecutor = annotationFederatedReadExecutor;

        final Principal p = ctx.getCallerPrincipal();
        final boolean isDatawavePrincipal = DatawavePrincipal.class.isAssignableFrom(p.getClass());
        final DatawavePrincipal dp = isDatawavePrincipal ? (DatawavePrincipal) p : null;

        this.userDn = dp != null ? dp.getUserDN().subjectDN() : p.getName();
        this.proxyServers = dp != null ? dp.getProxyServers() : null;
        this.datawavePrincipal = dp;

        // TODO: allow downgrading by reading query auths from query parameters.
        this.queryAuths = null;
    }

    /**
     * Calculate the authorizations for this request based on the principal and the queryAuths if any.
     *
     * @return a valid set of query auths, will throw an exception if this isn't possible.
     * @throws QueryException
     *             and exception if there was a problem calculating the auths
     */
    private Set<Authorizations> initializeAuthorizations() throws QueryException {
        if (authorizations == null) {
            log.trace("Initializing authorizations: userDn: {}, query: {}", userDn, queryAuths);
            if (datawavePrincipal == null) {
                throw new QueryException("Failed to get user principal from request, unable to proceed");
            }

            try {
                if (queryAuths == null) {
                    authorizations = AuthorizationsUtil.buildAuthorizations(datawavePrincipal.getAuthorizations());
                } else {
                    final String downgradedAuths = AuthorizationsUtil.downgradeUserAuths(queryAuths, datawavePrincipal, datawavePrincipal);
                    authorizations = AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(downgradedAuths)));
                }

            } catch (Exception e) {
                throw new QueryException("Failed to get user query authorizations", e);
            }

            log.debug("Authorizations initialized: userDn: {}, query: {}, final auths: {}", userDn, queryAuths, authorizations);
        }
        return authorizations;
    }

    /**
     * Initialize the accumulo client
     *
     * @return a valid client, will throw an exception if this isn't possibly
     * @throws QueryException
     *             if the client can't be initialized.
     */
    protected AccumuloClient initializeAccumuloClient() throws QueryException {
        if (client == null) {
            log.trace("Initializing accumulo client");
            UUID transactionUUID = java.util.UUID.randomUUID();

            if (connectionFactory == null) {
                throw new QueryException("The accumulo connection factory isn't present, unable to proceed");
            }

            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            if (trackingMap != null) {
                trackingMap.put("query.user", userDn);
                trackingMap.put("query.id", transactionUUID.toString());
                trackingMap.put("query.query", "annotation manager");
            } else {
                log.info("Accumulo connection tracking map was null, this isn't fatal, but odd.");
            }

            if (accumuloConnectionRequestBean == null) {
                throw new QueryException("The accumulo connection request manager isn't present, unable to proceed");
            }

            accumuloConnectionRequestBean.requestBegin(transactionUUID.toString(), userDn, trackingMap);
            try {
                client = connectionFactory.getClient(userDn, proxyServers, config.getConnPoolName(), config.getPriority(), trackingMap);
            } catch (Exception e) {
                throw new QueryException("Unable to get Accumulo client, exception encountered: ", e);
            } finally {
                accumuloConnectionRequestBean.requestEnd(transactionUUID.toString());
            }
            log.debug("Accumulo client initialized successfully");

        }
        return client;
    }

    /**
     * Initialize the lookup uuid service
     *
     * @return a valid lookup uuid service, throws an exception if this isn't possible.
     * @throws QueryException
     *             if the lookup uuid service can't be initialized.
     */
    protected LookupUUIDService initializeLookupUUIDService() throws QueryException {
        if (lookupUUIDService == null) {

            if (config == null) {
                throw new QueryException("The lookup uuid service configuration isn't present, unable to proceed");
            }

            if (responseObjectFactory == null) {
                throw new QueryException("The response object factory isn't present, unable to proceed");
            }

            log.trace("Initializing lookupUUIDService");
            final Set<Authorizations> authorizations = initializeAuthorizations();
            final AccumuloClient client = initializeAccumuloClient();
            lookupUUIDService = new LookupUUIDService(config.getLookupUUIDServiceConfig(), client, authorizations, responseObjectFactory,
                            config.getLookupUUIDQueryLogic());
            log.debug("LookupUUID service initialized successfully");
        }
        return lookupUUIDService;
    }

    /**
     * Initialize the annotation service, specifically the data access layer.
     *
     * @return a valid annotation data access object, throws an exception if this isn't possible.
     * @throws QueryException
     *             if the annotation data access object can't be initialized.
     */
    protected AnnotationReader initializeAnnotationService() throws QueryException {
        if (annotationDataAccess == null) {
            log.trace("Initializing annotation data access layer");
            final Set<Authorizations> authorizations = initializeAuthorizations();
            final AccumuloClient client = initializeAccumuloClient();
            final AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(config.getAnnotationConfig().getVisibilityTransformer(),
                            config.getAnnotationConfig().getTimestampTransformer());
            final AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer(
                            config.getAnnotationConfig().getVisibilityTransformer(), config.getAnnotationConfig().getTimestampTransformer());

            final AnnotationReader annotationReader = new AnnotationDataAccess(client, authorizations, config.getAnnotationConfig().getAnnotationTableName(),
                            config.getAnnotationConfig().getAnnotationSourceTableName(), annotationSerializer, annotationSourceSerializer);
            final AnnotationReader truthmarkReader = new AnnotationDataAccess(client, authorizations, config.getAnnotationConfig().getTruthmarkTableName(),
                            config.getAnnotationConfig().getTruthmarkSourceTableName(), annotationSerializer, annotationSourceSerializer);
            Map<String,AnnotationReader> annotationReaderMap = Map.of("annotation", annotationReader, "truthmark", truthmarkReader);
            annotationDataAccess = new FederatedAnnotationReader(annotationReaderMap, annotationFederatedReadExecutor);

            log.debug("Annotation data access layer initialized successfully");
        }
        return annotationDataAccess;
    }

    /**
     * Return the accumulo client currently held by this class. If there's a problem returning the client, logs a warning. Sets the internal client state to
     * null.
     */
    protected void returnAccumuloClient() {
        try {
            log.trace("Returning accumulo client");
            connectionFactory.returnClient(client);
            log.debug("Accumulo client returned");
        } catch (Exception e) {
            log.warn("Error when returning client", e);
        } finally {
            client = null;
        }
    }

    // --- ResponseRewriterContext implementation ---

    @Override
    public DatawavePrincipal getDatawavePrincipal() {
        return datawavePrincipal;
    }

    @Override
    public String getUserDn() {
        return userDn;
    }

    @Override
    public Collection<String> getProxyServers() {
        return proxyServers;
    }
}
