package datawave.webservice.query.util;

import java.security.Principal;
import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ejb.EJBContext;
import javax.ejb.EJBException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import datawave.query.data.UUIDType;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.util.time.DateHelper;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.QueryExecutor;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

/**
 * Utility for performing optimized queries based on UUIDs
 */
public class LookupUUIDUtil {

    protected static final String EMPTY_STRING = "";

    private static final String CONTENT_QUERY = "ContentQuery";
    private static final String DASH = "-";
    private static final String DOCUMENT_FIELD_NAME = "DOCUMENT:";
    private static final List<FieldBase> EMPTY_FIELDS = new ArrayList<>(0);
    private static final String EVENT_TYPE_NAME = "event";
    private static final String FORWARD_SLASH = "/";

    private static final String BANG = "!"; // you're dead!

    /**
     * Internally assigned parameter used to distinguish content lookup vs. regular query next(queryId) operations
     */
    public static final String PARAM_CONTENT_LOOKUP = "content.lookup";

    public static final String PARAM_HIT_LIST = "hit.list";
    private static final String PARAM_LUCENE_QUERY_SYNTAX = ";query.syntax:LUCENE-UUID";
    protected static final String QUOTE = "\"";
    private static final String REGEX_GROUPING_CHARS = "[()]";
    private static final String REGEX_NONWORD_CHARS = "[\\W&&[^:_\\.\\s-]]";
    private static final String REGEX_OR_OPERATOR = "[\\s][oO][rR][\\s]";
    private static final String REGEX_WHITESPACE_CHARS = "\\s";
    private static final String SPACE = " ";

    /**
     * The "defined view" for the UUIDType used for running queries based on internal metadata criteria (i.e., UID, row ID, and data type)
     */
    public static final String UID_QUERY = "UIDQuery";
    public static final String LOOKUP_UID_QUERY = "LookupUIDQuery";

    protected static final String UUID_TERM_DELIMITER = ":";
    private Date beginAsDate = null;

    private EJBContext ctx;

    private ResponseObjectFactory responseObjectFactory;

    private QueryLogicFactory queryLogicFactory;

    private AuditParameters auditParameters;

    private LookupUUIDConfiguration lookupUUIDConfiguration;
    private Logger log = Logger.getLogger(this.getClass());

    private int maxAllowedBatchLookupUUIDs = LookupUUIDConstants.DEFAULT_BATCH_LOOKUP_UPPER_LIMIT;

    private final QueryExecutor queryExecutor;

    private final UserOperations userOperations;

    private Map<String,UUIDType> uuidTypes = Collections.synchronizedMap(new HashMap<>());

    MultivaluedMap<String,String> defaultOptionalParams;

    /**
     * Constructor
     *
     * @param configuration
     *            Configuration bean for lookupUUID web service endpoints
     * @param queryExecutor
     *            Service that executes queriesoptionalParamsToMap
     * @param context
     *            The EJB's content
     * @param queryLogicFactory
     *            the query factory
     * @param responseObjectFactory
     *            the response object factory
     * @param userOperations
     *            the user operations
     */
    public LookupUUIDUtil(final LookupUUIDConfiguration configuration, final QueryExecutor queryExecutor, final EJBContext context,
                    final ResponseObjectFactory responseObjectFactory, final QueryLogicFactory queryLogicFactory, final UserOperations userOperations) {
        // Validate and assign the lookup UUID configuration
        if (null == configuration) {
            throw new IllegalArgumentException("Non-null configuration required to lookup UUIDs");
        }
        this.lookupUUIDConfiguration = configuration;

        // Validate and assign the query executor
        if (null == queryExecutor) {
            throw new IllegalArgumentException("Non-null query executor required to lookup UUIDs");
        }
        this.queryExecutor = queryExecutor;

        this.userOperations = userOperations;

        // Assign the EJB context
        this.ctx = context;

        // Assign the field event factory needed for the response objects
        this.responseObjectFactory = responseObjectFactory;

        // set the query logic factory
        this.queryLogicFactory = queryLogicFactory;

        // Populate the UUIDType map
        final List<UUIDType> types = this.lookupUUIDConfiguration.getUuidTypes();
        this.uuidTypes.clear();
        if (null != types) {
            for (final UUIDType type : types) {
                if (null != type) {
                    this.uuidTypes.put(type.getFieldName().toUpperCase(), type);
                }
            }
        }

        // Assign the begin date
        try {
            this.beginAsDate = DateHelper.parseWithGMT(this.lookupUUIDConfiguration.getBeginDate());
        } catch (DateTimeParseException e) {
            this.log.error(e.getMessage(), e);
        }

        // Assign the maximum number of UUIDs allowed for batch lookup. A zero or negative
        // value is interpreted as unlimited, which is automatically adjusted to -1.
        this.maxAllowedBatchLookupUUIDs = this.lookupUUIDConfiguration.getBatchLookupUpperLimit();
        if (this.maxAllowedBatchLookupUUIDs <= 0) {
            this.maxAllowedBatchLookupUUIDs = -1;
        }

        this.defaultOptionalParams = this.lookupUUIDConfiguration.optionalParamsToMap();
    }

    /*
     * Create manageable batches of contentQuery strings based on the configured upper limit of UUIDS, if any. A content query term from one item in the list
     * would look like the following example: <p> "DOCUMENT:shardId/datatype/uid" <p> <b>Note:</b> An attempt was made to concatenate multiple events into a
     * single OR'd expression, but the ContentQueryTable only supports one term at a time.
     *
     * @param eventResponse a specialized response for optimizing content lookup based on internal event IDs
     *
     * @return a list of batched content query strings
     */
    private List<StringBuilder> createContentQueryStrings(final AbstractUUIDLookupCriteria validatedCriteria, boolean multiTermExpressionsSupported) {
        // Initialize the returned list of query strings
        final List<StringBuilder> batchedContentQueryStrings = new LinkedList<>();

        // Get the raw query string from the validated criteria
        final String rawQueryString = validatedCriteria.getRawQueryString();

        // Initialize the string builder
        StringBuilder contentQuery = null;
        int eventCounter = 0;

        // Break apart into separate terms
        final String[] uuidTypeValuePairs = rawQueryString.split(REGEX_WHITESPACE_CHARS);
        for (final String potentialUUIDTerm : uuidTypeValuePairs) {
            // Double-check for the expected type/value delimiter (i.e., event:shardID/datatype/uid)
            if (potentialUUIDTerm.contains(UUID_TERM_DELIMITER)) {
                // Get the UUID type and value
                final String[] splitPair = potentialUUIDTerm.split(UUID_TERM_DELIMITER);
                final String uuidType = splitPair[0].trim().toUpperCase();
                final String uuid;
                if (splitPair.length > 1) {
                    uuid = splitPair[1].trim();
                } else {
                    uuid = null;
                }

                if (EVENT_TYPE_NAME.equalsIgnoreCase(uuidType) && (null != uuid)) {
                    // Conditionally initialize a new query string and the event counter
                    if ((null == contentQuery) || (!multiTermExpressionsSupported)
                                    || ((this.maxAllowedBatchLookupUUIDs > 0) && (eventCounter > this.maxAllowedBatchLookupUUIDs))) {
                        contentQuery = new StringBuilder();
                        batchedContentQueryStrings.add(contentQuery);
                        eventCounter = 1;
                    }
                    // Conditionally append an OR operator
                    else if (contentQuery.length() > 0) {
                        contentQuery.append(SPACE);
                    }

                    // Append the content query criteria
                    contentQuery.append(DOCUMENT_FIELD_NAME);
                    contentQuery.append(uuid);
                }
            }
        }

        return batchedContentQueryStrings;
    }

    /*
     * Create contentQuery strings based on the specified events, if any. A contentQuery string from one event would look like the following example: <p>
     * "DOCUMENT:shardId/datatype/uid" <p> <b>Note:</b> An attempt was made to concatenate multiple events into a single OR'd expression, but the
     * ContentQueryTable only supports one term at a time.
     *
     * @param eventResponse the response from the UUID query
     *
     * @return a list of batched content query strings
     */
    private List<StringBuilder> createContentQueryStrings(final EventQueryResponseBase eventResponse) {
        // Initialize a flag that can be flipped to support multi-term ContentQueryTable expressions
        // at some unknown point in the future
        boolean multiTermExpressionsSupported = true;

        // Declare the returned list of query strings
        final List<StringBuilder> contentQueryStrings;

        // Build query strings based on an optimized all-event response
        if (eventResponse instanceof AllEventMockResponse) {
            final AllEventMockResponse allEventResponse = (AllEventMockResponse) eventResponse;
            final AbstractUUIDLookupCriteria validatedCriteria = allEventResponse.getLookupCriteria();
            contentQueryStrings = this.createContentQueryStrings(validatedCriteria, multiTermExpressionsSupported);
        }
        // Otherwise, handle "normally" built query strings
        else {
            // Create the list
            contentQueryStrings = new LinkedList<>();

            // Get the entire list of queried events
            final List<EventBase> unbatchedEvents = eventResponse.getEvents();

            // Iterate through the unbatched events and fill the batched set of query strings
            StringBuilder contentQuery = null;
            int eventCounter = 0;
            for (final EventBase<?,?> event : unbatchedEvents) {
                // Get the event's shard table info
                final Metadata metadata = event.getMetadata();
                final String row = metadata.getRow();
                final String dataType = metadata.getDataType();
                final String internalId = metadata.getInternalId();

                String identifier = null;
                List<FieldBase> fields = (List<FieldBase>) event.getFields();
                if (fields != null) {
                    for (FieldBase fb : fields) {
                        if (fb.getName().equals("HIT_TERM")) {
                            identifier = fb.getValueString();
                        }
                    }
                }

                // Increment the counter
                eventCounter++;

                // Conditionally initialize a new query string and the event counter
                if ((null == contentQuery) || (!multiTermExpressionsSupported)
                                || ((this.maxAllowedBatchLookupUUIDs > 0) && (eventCounter > this.maxAllowedBatchLookupUUIDs))) {
                    contentQuery = new StringBuilder();
                    contentQueryStrings.add(contentQuery);
                    eventCounter = 1;
                }
                // Conditionally append an OR operator
                else if (contentQuery.length() > 0) {
                    contentQuery.append(SPACE);
                }

                // Append the content query criteria
                contentQuery.append(DOCUMENT_FIELD_NAME).append(row);
                contentQuery.append(FORWARD_SLASH).append(dataType);
                contentQuery.append(FORWARD_SLASH).append(internalId);
                if (identifier != null) {
                    contentQuery.append(BANG);
                    contentQuery.append(identifier);
                }
            }
        }

        return contentQueryStrings;
    }

    /**
     * Creates a UUID query and returns the "first" available set of results. If the specified criteria contains a valid, non-null HttpHeaders value and the
     * query is able to find matching Events, a StreamingOutput instance will be returned that allows all content to be streamed back to the caller.
     *
     * @param unvalidatedCriteria
     *            UUID lookup criteria that has presumably not been validated
     * @param <T>
     *            type of response
     * @return a BaseQueryResponse if the criteria contains a null HttpHeaders value (indicating paged results are required), or StreamingOutput if a valid,
     *         non-null HttpHeaders value is provided
     */
    @SuppressWarnings("unchecked")
    public <T> T createUUIDQueryAndNext(final AbstractUUIDLookupCriteria unvalidatedCriteria) {

        // Declare the returned response
        final T response;

        // Find out who/what called this method
        final Principal principal = this.ctx.getCallerPrincipal();
        String sid = principal.getName();

        // Validate the lookup criteria and get its HTTP headers, which may be null
        final AbstractUUIDLookupCriteria validatedCriteria = this.validateLookupCriteria(unvalidatedCriteria, true);
        final HttpHeaders headers = validatedCriteria.getStreamingOutputHeaders();

        // If the criteria is intended for content lookup and contains only UIDQuery "event" types, allow
        // for optimized content lookup by skipping the UUID lookup query and assigning a "dummy" EventQueryResponseBase.
        if (validatedCriteria.isContentLookup() && validatedCriteria.isAllEventLookup()) {
            response = (T) new AllEventMockResponse(validatedCriteria);
        }
        // Otherwise, just execute the query as normally expected
        else {
            // Get the validated query details
            MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
            queryParameters.putAll(this.defaultOptionalParams);
            queryParameters.putAll(validatedCriteria.getQueryParameters());
            queryParameters.putSingle(QueryParameters.QUERY_STRING, validatedCriteria.getRawQueryString());

            // Override the extraneous query details
            String logicName = queryParameters.getFirst(QueryParameters.QUERY_LOGIC_NAME);
            String queryAuths = queryParameters.getFirst(QueryParameters.QUERY_AUTHORIZATIONS);
            String userAuths = getAuths(logicName, queryParameters, queryAuths, principal);
            if (queryParameters.containsKey(QueryParameters.QUERY_AUTHORIZATIONS)) {
                queryParameters.remove(QueryParameters.QUERY_AUTHORIZATIONS);
            }
            queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);

            final String queryName = sid + DASH + UUID.randomUUID();
            if (queryParameters.containsKey(QueryParameters.QUERY_NAME)) {
                queryParameters.remove(QueryParameters.QUERY_NAME);
            }
            queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);

            try {
                queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(this.beginAsDate));
            } catch (ParseException e) {
                throw new RuntimeException("Unable to format new query begin date: " + this.beginAsDate);
            }

            final Date endDate = DateUtils.addDays(new Date(), 2);
            if (queryParameters.containsKey(QueryParameters.QUERY_END)) {
                queryParameters.remove(QueryParameters.QUERY_END);
            }
            try {
                queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
            } catch (ParseException e) {
                throw new RuntimeException("Unable to format new query end date: " + endDate);
            }

            final Date expireDate = new Date(endDate.getTime() + 1000 * 60 * 60);
            if (queryParameters.containsKey(QueryParameters.QUERY_EXPIRATION)) {
                queryParameters.remove(QueryParameters.QUERY_EXPIRATION);
            }
            try {
                queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expireDate));
            } catch (ParseException e) {
                throw new RuntimeException("Unable to format new query expr date: " + expireDate);
            }
            queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, QueryPersistence.TRANSIENT.name());
            queryParameters.putSingle(QueryParameters.QUERY_TRACE, "false");

            // If the headers are defined as part of a standard UUID lookup, execute the query for a streamed response
            if (!validatedCriteria.isContentLookup() && (null != headers)) {
                response = (T) this.queryExecutor.execute(logicName, queryParameters, validatedCriteria.getStreamingOutputHeaders());
            }
            // Otherwise, execute the query for a standard paged response
            else {
                response = (T) this.queryExecutor.createQueryAndNext(logicName, queryParameters);
            }
        }

        // Handle non-optimized content lookup
        if (validatedCriteria.isContentLookup() && (response instanceof BaseQueryResponse)) {
            // Validate for the expected response implementation
            final EventQueryResponseBase pagedResponse = this.validatePagedResponse((BaseQueryResponse) response);

            // Don't do any work unless there are results
            if (pagedResponse.getHasResults()) {
                // Strip all unnecessary event information
                final List<EventBase> events = this.removeIrrelevantEventInformation(pagedResponse.getEvents());
                pagedResponse.setEvents(events);

                // If streamed output is required, perform all available next(queryId) operations and
                // reconstruct the response to include their basic event info.
                if (null != headers) {
                    this.mergeNextUUIDLookups(pagedResponse);
                }
            }
        }
        return response;
    }

    public Query createSettings(MultivaluedMap<String,String> queryParameters) {
        log.debug("Initial query parameters: " + queryParameters);
        Query query = responseObjectFactory.getQueryImpl();
        if (queryParameters != null) {
            MultivaluedMap<String,String> expandedQueryParameters = new MultivaluedMapImpl<>();
            if (defaultOptionalParams != null) {
                expandedQueryParameters.putAll(defaultOptionalParams);
            }
            String delimitedParams = queryParameters.getFirst(QueryParameters.QUERY_PARAMS);
            if (delimitedParams != null) {
                for (QueryImpl.Parameter pm : QueryUtil.parseParameters(delimitedParams)) {
                    expandedQueryParameters.putSingle(pm.getParameterName(), pm.getParameterValue());
                }
            }
            expandedQueryParameters.putAll(queryParameters);
            log.debug("Final query parameters: " + expandedQueryParameters);
            query.setOptionalQueryParameters(expandedQueryParameters);
            for (String key : expandedQueryParameters.keySet()) {
                if (expandedQueryParameters.get(key).size() == 1) {
                    query.addParameter(key, expandedQueryParameters.getFirst(key));
                }
            }
        }
        return query;
    }

    public String getAuths(String logicName, MultivaluedMap<String,String> queryParameters, String queryAuths, Principal principal) {
        String userAuths;
        try {
            QueryLogic<?> logic = queryLogicFactory.getQueryLogic(logicName, principal);
            Query settings = createSettings(queryParameters);
            if (queryAuths == null) {
                logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(((DatawavePrincipal)principal).getAuthorizations()));
            } else {
                logic.preInitialize(settings, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(WSAuthorizationsUtil.splitAuths(queryAuths))));
            }
            // the query principal is our local principal unless the query logic has a different user operations
            DatawavePrincipal queryPrincipal = (logic.getUserOperations() == null) ? (DatawavePrincipal) principal
                            : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal);
            // the overall principal (the one with combined auths across remote user operations) is our own user operations (probably the UserOperationsBean)
            DatawavePrincipal overallPrincipal = (userOperations == null) ? (DatawavePrincipal) principal
                            : userOperations.getRemoteUser((DatawavePrincipal) principal);
            if (queryAuths != null) {
                userAuths = WSAuthorizationsUtil.downgradeUserAuths(queryAuths, overallPrincipal, queryPrincipal);
            } else {
                userAuths = WSAuthorizationsUtil.buildUserAuthorizationString(queryPrincipal);
            }
        } catch (Exception e) {
            log.error("Failed to get user query authorizations", e);
            throw new DatawaveWebApplicationException(e, new VoidResponse());
        }
        return userAuths;
    }

    /**
     * Returns a UUIDType implementation, if any, matching the specified field name
     *
     * @param uuidType
     *            the field name of the desired UUIDType
     * @return a UUIDType implementation, if any, matching the specified field name
     */
    public UUIDType getUUIDType(final String uuidType) {
        final UUIDType type;
        if (null != uuidType) {
            type = this.uuidTypes.get(uuidType.toUpperCase());
        } else {
            type = null;
        }

        return type;
    }

    /**
     * Returns the EJB context that was active when this class was created.
     *
     * @return the context
     */
    public EJBContext getContext() {
        return ctx;
    }

    /**
     * Lookup content based on the Events obtained from a {@link BaseQueryResponse} returned from a <code>QueryExecutor.next(queryId)</code> operation.
     *
     * @param validatedCriteria
     *            pre-validated UUID lookup criteria
     * @param nextQueryResponse
     *            the results of a <code>QueryExecutor.next(queryId)</code> operation
     * @param <T>
     *            type of response
     * @return a BaseQueryResponse if the criteria contains a null HttpHeaders value (indicating paged results are required), or StreamingOutput if a valid,
     *         non-null HttpHeaders value is provided
     */
    @SuppressWarnings("unchecked")
    public <T> T lookupContentByNextResponse(final AbstractUUIDLookupCriteria validatedCriteria, final BaseQueryResponse nextQueryResponse) {
        // Initialize the returned response
        T contentQueryResponse = null;

        // Query for the relevant content
        try {
            // Handle the case where /next has already returned results of a content query, which
            // can occur when the initial UUID lookup is based entirely on UIDQuery event IDs.
            //
            // Note: The instanceof expression below is merely for human readability and should never actually
            // be evaluated since the getLogicName() expression would always evaluate as false for an instance
            // of AllEventMockResponse.
            //
            if (CONTENT_QUERY.equals(nextQueryResponse.getLogicName()) && !(nextQueryResponse instanceof AllEventMockResponse)) {
                contentQueryResponse = (T) nextQueryResponse;
            }
            // Handle the case where /next has returned results of a UUID lookup, and a secondary content
            // lookup is required
            else {
                // Create and execute one or more queries based on the UUID-queried events
                contentQueryResponse = this.lookupContentByEvents(validatedCriteria, nextQueryResponse);
            }
        }
        // Modify the merged content response to allow for /next UUID paging
        finally {
            if ((contentQueryResponse instanceof BaseQueryResponse) && !(nextQueryResponse instanceof AllEventMockResponse)) {
                final BaseQueryResponse pagedResponse = (BaseQueryResponse) contentQueryResponse;
                pagedResponse.setPartialResults(nextQueryResponse.isPartialResults());
                pagedResponse.setPageNumber(nextQueryResponse.getPageNumber());
                pagedResponse.setQueryId(nextQueryResponse.getQueryId());
            }
        }

        return contentQueryResponse;
    }

    /**
     * Lookup content based on presumably non-validated criteria
     *
     * @param unvalidatedCriteria
     *            UUID lookup criteria that has presumably not been validated
     * @param <T>
     *            type of response
     * @return a BaseQueryResponse if the criteria contains a null HttpHeaders value (indicating paged results are required), or StreamingOutput if a valid,
     *         non-null HttpHeaders value is provided
     */
    public <T> T lookupContentByUUIDs(final AbstractUUIDLookupCriteria unvalidatedCriteria) {
        // Initialize the query responses
        BaseQueryResponse nextQueryResponse = null;
        T contentQueryResponse = null;

        // Lookup the next set of UUID-queried events
        if (unvalidatedCriteria instanceof NextContentCriteria) {
            final String queryId = unvalidatedCriteria.getRawQueryString();
            nextQueryResponse = this.queryExecutor.next(queryId);
        }
        // Otherwise, perform the initial lookup of UUID-queried events
        else {
            // Set the content lookup flag
            unvalidatedCriteria.setContentLookup(true);

            // Create the UUID query
            nextQueryResponse = this.createUUIDQueryAndNext(unvalidatedCriteria);
        }

        // Lookup content using the next response
        contentQueryResponse = this.lookupContentByNextResponse(unvalidatedCriteria, nextQueryResponse);

        return contentQueryResponse;
    }

    /*
     * Lookup content based on the events, if any, contained in the specified BaseQueryResponse
     *
     * @param criteria presumably valid lookup criteria
     *
     * @param uuidQueryResponse the results of a UUID lookup query
     *
     * @return a BaseQueryResponse if the criteria contains a null HttpHeaders value, or StreamingOutput if a valid, non-null HttpHeaders value is provided
     */
    @SuppressWarnings("unchecked")
    private <T> T lookupContentByEvents(final AbstractUUIDLookupCriteria criteria, final BaseQueryResponse uuidQueryResponse) {
        // Initialize the return value
        T contentResponse = null;

        // Validate for the expected response implementation
        final EventQueryResponseBase eventResponse = this.validatePagedResponse(uuidQueryResponse);

        // Find out who/what called this method
        final Principal principal = this.ctx.getCallerPrincipal();
        String sid = principal.getName();

        // Initialize the reusable query input
        final String queryName = sid + '-' + UUID.randomUUID();
        final Date endDate = new Date();
        final Date expireDate = new Date(endDate.getTime() + 1000 * 60 * 60);

        // Create manageable batches of contentQuery strings based on the configured upper limit of UUIDS, if any
        final List<StringBuilder> batchedContentQueryStrings = this.createContentQueryStrings(eventResponse);

        // Perform criteria validation if paging through next results
        final AbstractUUIDLookupCriteria validatedCriteria;
        if (criteria instanceof NextContentCriteria) {
            validatedCriteria = this.validateLookupCriteria(criteria, false);
        } else {
            validatedCriteria = criteria;
        }
        final String userAuths = getAuths(CONTENT_QUERY, validatedCriteria.getQueryParameters(), null, principal);

        // Perform the lookup
        boolean allEventMockResponse = (uuidQueryResponse instanceof AllEventMockResponse);
        try {
            if (null != validatedCriteria.getStreamingOutputHeaders()) {
                contentResponse = (T) this.lookupStreamedContent(queryName, validatedCriteria, batchedContentQueryStrings, endDate, expireDate, userAuths,
                                allEventMockResponse);
            } else {
                contentResponse = (T) this.lookupPagedContent(queryName, validatedCriteria, batchedContentQueryStrings, endDate, expireDate, userAuths,
                                allEventMockResponse);
            }
        } catch (NoResultsException e) {
            // close the original lookupId query and re-throw
            queryExecutor.close(uuidQueryResponse.getQueryId());
            throw e;
        }

        return contentResponse;
    }

    private EventQueryResponseBase lookupPagedContent(final String queryName, final AbstractUUIDLookupCriteria validatedCriteria,
                    final List<StringBuilder> batchedContentQueryStrings, final Date endDate, final Date expireDate, final String userAuths,
                    boolean allEventMockResponse) {
        // Initialize the return value
        EventQueryResponseBase mergedContentQueryResponse = null;

        // Call the ContentQuery for one or more events
        DatawaveWebApplicationException noResultsException = null;
        for (final StringBuilder contentQuery : batchedContentQueryStrings) {
            // Submitted query should look like this:
            //
            // DOCUMENT:shardId/datatype/uid [DOCUMENT:shardId/datatype/uid]*
            //
            MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
            queryParameters.putAll(this.defaultOptionalParams);
            queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
            queryParameters.putSingle(QueryParameters.QUERY_STRING, contentQuery.toString());
            try {
                queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(this.beginAsDate));
            } catch (ParseException e1) {
                throw new RuntimeException("Error formatting begin date: " + this.beginAsDate);
            }
            try {
                queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
            } catch (ParseException e1) {
                throw new RuntimeException("Error formatting end date: " + endDate);
            }
            queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);
            try {
                queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expireDate));
            } catch (ParseException e1) {
                throw new RuntimeException("Error formatting expr date: " + expireDate);
            }
            queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, QueryPersistence.TRANSIENT.name());
            queryParameters.putSingle(QueryParameters.QUERY_TRACE, "false");

            for (String key : validatedCriteria.getQueryParameters().keySet()) {
                if (!queryParameters.containsKey(key)) {
                    queryParameters.put(key, validatedCriteria.getQueryParameters().get(key));
                }
            }

            final GenericResponse<String> createResponse = this.queryExecutor.createQuery(CONTENT_QUERY, queryParameters);
            final String contentQueryId = createResponse.getResult();
            boolean preventCloseOfMergedQueryId = ((null == mergedContentQueryResponse) && allEventMockResponse);
            try {
                BaseQueryResponse contentQueryResponse = null;
                do {
                    try {
                        // Get the first/next results
                        contentQueryResponse = this.queryExecutor.next(contentQueryId);

                        // Validate the response, which also checks for null
                        if (!(contentQueryResponse instanceof EventQueryResponseBase)) {
                            EventQueryResponseBase er = responseObjectFactory.getEventQueryResponse();
                            er.addMessage("Unhandled response type " + contentQueryResponse + " from ContentQuery");
                            throw new DatawaveWebApplicationException(new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS,
                                            "Expected EventQueryResponseBase but got " + contentQueryResponse.getClass()), er);
                        }

                        // Prevent NPE due to attempted merge when total events is null
                        final EventQueryResponseBase eventQueryResponse = (EventQueryResponseBase) contentQueryResponse;
                        if (null == eventQueryResponse.getTotalEvents()) {
                            final Long returnedEvents = eventQueryResponse.getReturnedEvents();
                            eventQueryResponse.setTotalEvents((null != returnedEvents) ? returnedEvents : 0L);
                        }

                        // Assign the merged response if it hasn't been done yet
                        if (null == mergedContentQueryResponse) {
                            mergedContentQueryResponse = (EventQueryResponseBase) contentQueryResponse;
                        }
                        // If the merged content has already been assigned, merge into it, but keep the original
                        // query Id
                        else {
                            final String queryId = mergedContentQueryResponse.getQueryId();
                            mergedContentQueryResponse.merge((EventQueryResponseBase) contentQueryResponse);
                            mergedContentQueryResponse.setQueryId(queryId);
                        }
                    } catch (final NoResultsException e) {
                        contentQueryResponse = null;
                        noResultsException = e;
                    }
                    // This used to be the case. Don't know when the executor started
                    // directly throwing a NoResultsException, but this is kept just
                    // in case.
                    catch (final EJBException e) {
                        final Throwable cause = e.getCause();
                        if (cause instanceof DatawaveWebApplicationException) {
                            DatawaveWebApplicationException nwae = (DatawaveWebApplicationException) cause;
                            if (nwae instanceof NoResultsException) {
                                contentQueryResponse = null;
                                noResultsException = nwae;
                            } else {
                                throw nwae;
                            }
                        }
                    }
                }
                // Loop if more results are available
                while (null != contentQueryResponse);
            } finally {
                if (!preventCloseOfMergedQueryId) {
                    this.queryExecutor.close(contentQueryId);
                }
            }
        }

        // Conditionally throw a NoResultsException
        if ((null == mergedContentQueryResponse) && (null != noResultsException)) {
            throw noResultsException;
        }

        return mergedContentQueryResponse;
    }

    private StreamingOutput lookupStreamedContent(final String queryName, final AbstractUUIDLookupCriteria validatedCriteria,
                    final List<StringBuilder> batchedContentQueryStrings, final Date endDate, final Date expireDate, final String userAuths,
                    boolean allEventMockResponse) {

        // Merge the content query strings
        final StringBuilder contentQuery = new StringBuilder();
        for (final StringBuilder contentQueryString : batchedContentQueryStrings) {
            if (contentQuery.length() > 0) {
                contentQuery.append(' ');
            }
            contentQuery.append(contentQueryString);
        }
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(this.defaultOptionalParams);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, contentQuery.toString());
        try {
            queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(this.beginAsDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting begin date: " + this.beginAsDate);
        }
        try {
            queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting end date: " + endDate);
        }
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, userAuths);
        try {
            queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expireDate));
        } catch (ParseException e1) {
            throw new RuntimeException("Error formatting expr date: " + expireDate);
        }
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, QueryPersistence.TRANSIENT.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, "false");

        for (String key : validatedCriteria.getQueryParameters().keySet()) {
            if (!queryParameters.containsKey(key)) {
                queryParameters.put(key, validatedCriteria.getQueryParameters().get(key));
            }
        }

        // Call the ContentQuery for one or more events
        final HttpHeaders headers = validatedCriteria.getStreamingOutputHeaders();
        return this.queryExecutor.execute(CONTENT_QUERY, queryParameters, headers);
    }

    private void mergeNextUUIDLookups(final EventQueryResponseBase mergedResponse) {
        // Get the query ID in order to perform the next(queryID) operations
        final String queryId = mergedResponse.getQueryId();

        // Initialize a list to collect all queried events
        final List<EventBase> mergedEvents = new LinkedList<>(mergedResponse.getEvents());

        // Get each set of next events, remove irrelevant information, and merge them into the main response
        try {
            EventQueryResponseBase eventResponse = null;
            do {
                final BaseQueryResponse nextResponse = this.queryExecutor.next(queryId);
                eventResponse = this.validatePagedResponse(nextResponse);
                final List<EventBase> nextEvents = this.removeIrrelevantEventInformation(eventResponse.getEvents());
                mergedEvents.addAll(nextEvents);
            } while (null != eventResponse);
        } catch (final NoResultsException e) {
            // No op
        } finally {
            try {
                this.queryExecutor.close(queryId);
            } catch (final Exception e) {
                final String message = "Unable to close UUID lookup query " + queryId + " while performing a content lookup";
                log.error(message, e);
            }
        }

        // Apply the merged fields back to the merged response
        mergedResponse.setEvents(mergedEvents);
    }

    private List<EventBase> removeIrrelevantEventInformation(final List<EventBase> events) {
        final List<EventBase> guttedEvents;
        if (null != events) {
            guttedEvents = events;
            for (final EventBase event : events) {
                if (null != event && null != event.getFields()) {
                    final Iterator<FieldBase> it = (Iterator<FieldBase>) event.getFields().iterator();
                    while (it.hasNext()) {
                        final FieldBase fb = it.next();
                        // hit term is used for enriching the content with the identifier used to retrieve it.
                        if (!fb.getName().equals("HIT_TERM")) {
                            it.remove();
                        }
                    }
                }
            }
        } else {
            guttedEvents = new ArrayList<>(0);
        }

        return guttedEvents;
    }

    private AbstractUUIDLookupCriteria validateLookupCriteria(final AbstractUUIDLookupCriteria criteria, boolean validateUUIDTerms) {
        // Initialize the validated logic name, which is only necessary for UUID lookup and
        // OK to be a null value when paging through content results.
        String logicName = null;

        // Conditionally validate UUID type/value pairs
        if (validateUUIDTerms) {
            // Get the unvalidated LUCENE query for UUID lookup
            final String unvalidatedQuery = criteria.getRawQueryString();

            // Initialize the counter for validating against the maximum number of allowed UUIDs
            int uuidPairCount = 0;
            int eventTypeCountForContentLookup = 0;

            // Reformat the query into a tokenizable series of UUID type/value pairs
            String tokenizablePairs;
            if (null != unvalidatedQuery) {
                tokenizablePairs = unvalidatedQuery;
                tokenizablePairs = tokenizablePairs.replaceAll(REGEX_GROUPING_CHARS, SPACE); // Replace grouping characters with whitespace
                tokenizablePairs = tokenizablePairs.replaceAll(REGEX_NONWORD_CHARS, EMPTY_STRING); // Remove most, but not all, non-word characters
                tokenizablePairs = tokenizablePairs.replaceAll(REGEX_OR_OPERATOR, SPACE); // Remove OR operators
            } else {
                tokenizablePairs = EMPTY_STRING;
            }

            // Validate each UUID type and value
            final String[] uuidTypeValuePairs = tokenizablePairs.split(REGEX_WHITESPACE_CHARS);
            for (final String potentialUUIDTerm : uuidTypeValuePairs) {
                // Validate the "potential" UUID term. It's potential because it could be an OR operator
                // or some other query syntax that would be validated with more scrutiny once the query
                // executor is invoked.
                final UUIDType uuidType = this.validateUUIDTerm(potentialUUIDTerm.trim(), logicName);
                if (null != uuidType) {
                    // Assign the query logic name if undefined
                    if (null == logicName) {
                        logicName = uuidType.getDefinedView();
                    }

                    // Increment the UUID type/value count
                    uuidPairCount++;

                    // Increment the counter for specialized "event" UUID types in the case of content lookups
                    if (criteria.isContentLookup() && EVENT_TYPE_NAME.equals(uuidType.getFieldName())) {
                        eventTypeCountForContentLookup++;
                    }
                }
            }

            // Validate at least one UUID was specified in the query string
            if (null == logicName) {
                final String message = "Undefined UUID types not supported with the LuceneToJexlUUIDQueryParser";
                final GenericResponse<String> errorReponse = new GenericResponse<>();
                errorReponse.addMessage(message);
                throw new DatawaveWebApplicationException(new IllegalArgumentException(message), errorReponse);
            }

            // Validate the number of specified UUIDs did not exceed the upper limit, if any
            if ((this.maxAllowedBatchLookupUUIDs > 0) && (uuidPairCount > this.maxAllowedBatchLookupUUIDs)) {
                final String message = "The " + uuidPairCount + " specified UUIDs exceed the maximum number of " + this.maxAllowedBatchLookupUUIDs
                                + " allowed for a given lookup request";
                final GenericResponse<String> errorReponse = new GenericResponse<>();
                errorReponse.addMessage(message);
                throw new DatawaveWebApplicationException(new IllegalArgumentException(message), errorReponse);
            }

            // Set the flag if we know we're dealing with an all-event UID lookup that has not exceeded the max page size
            if ((eventTypeCountForContentLookup > 0) && (uuidPairCount == eventTypeCountForContentLookup)
                            && (uuidPairCount <= Integer.parseInt(criteria.getQueryParameters().getFirst(QueryParameters.QUERY_PAGESIZE)))) {
                criteria.setAllEventLookup(true);
            }
        }

        // Set the query logic
        criteria.getQueryParameters().put(QueryParameters.QUERY_LOGIC_NAME, Collections.singletonList(logicName));

        List<String> paramList = criteria.getQueryParameters().remove(QueryParameters.QUERY_PARAMS);
        String params = null;
        if (paramList != null && !paramList.isEmpty()) {
            params = paramList.get(0);
        }
        // Add Lucene syntax to the parameters, except during a call for next content
        if (!(criteria instanceof NextContentCriteria)) {
            params = params + PARAM_LUCENE_QUERY_SYNTAX;
        }

        // Conditionally add content.lookup syntax to parameters to indicate content lookup during "next" calls
        if (criteria.isContentLookup() && !criteria.isAllEventLookup()) {
            params = params + ';' + PARAM_CONTENT_LOOKUP + ':' + true;
        }

        // Required so that we can return identifiers alongside the content returned in the content lookup.
        if (criteria.isContentLookup()) {
            params = params + ';' + PARAM_HIT_LIST + ':' + true;
        }

        criteria.getQueryParameters().putSingle(QueryParameters.QUERY_PARAMS, params);

        // All is well, so return the validated criteria
        return criteria;
    }

    private EventQueryResponseBase validatePagedResponse(final BaseQueryResponse response) {
        final EventQueryResponseBase pagedResponse;
        if (response instanceof EventQueryResponseBase) {
            pagedResponse = (EventQueryResponseBase) response;
        } else {
            final EventQueryResponseBase er = responseObjectFactory.getEventQueryResponse();
            er.addMessage("Unhandled response type from Query/createQueryAndNext");
            throw new DatawaveWebApplicationException(
                            new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS, "Expected EventQueryResponseBase but got " + response.getClass()), er);
        }

        return pagedResponse;
    }

    /*
     * Validate the specified token as a UUID lookup term, either as a LUCENE-formatted field/value or a UIDQuery field/value. Tokens missing the appropriate
     * delimiter are ignored and return with a null UUIDType.
     *
     * @param uuidTypeValueTerm A token to evaluate as a possible UUID field/value term
     *
     * @param logicName The existing assigned query logic name, if any
     *
     * @return A valid UUIDType, or null if the specified token is obviously not a UUID field/value term
     */
    private UUIDType validateUUIDTerm(final String possibleUUIDTerm, final String logicName) {
        // Declare the return value
        final UUIDType matchingUuidType;

        // Check for the expected type/value delimiter (i.e., UUIDType:UUID)
        if (possibleUUIDTerm.contains(UUID_TERM_DELIMITER)) {
            final String[] splitPair = possibleUUIDTerm.split(UUID_TERM_DELIMITER);
            final String uuidType = splitPair[0].trim().toUpperCase();
            final String uuid;
            if (splitPair.length > 1) {
                uuid = splitPair[1].trim();
            } else {
                uuid = null;
            }

            // Get the matching UUID type
            matchingUuidType = this.uuidTypes.get(uuidType.toUpperCase());

            // Validate UUID type
            if (null == matchingUuidType) {
                final String message = "Invalid type '" + uuidType + "' for UUID " + uuid + " not supported with the LuceneToJexlUUIDQueryParser";
                final GenericResponse<String> errorReponse = new GenericResponse<>();
                errorReponse.addMessage(message);
                throw new DatawaveWebApplicationException(new IllegalArgumentException(message), errorReponse);
            }
            // Validate the UUID value
            else if ((null == uuid) || uuid.isEmpty()) {
                final String message = "Undefined UUID using type '" + uuidType + "' not supported with the LuceneToJexlUUIDQueryParser";
                final GenericResponse<String> errorReponse = new GenericResponse<>();
                errorReponse.addMessage(message);
                throw new DatawaveWebApplicationException(new IllegalArgumentException(message), errorReponse);
            }
            // Reject conflicting logic name
            else if ((null != logicName) && !logicName.equals(matchingUuidType.getDefinedView())) {
                final String message = "Multiple UUID types '" + logicName + "' and '" + matchingUuidType.getDefinedView() + "' not "
                                + " supported within the same lookup request";
                final GenericResponse<String> errorReponse = new GenericResponse<>();
                errorReponse.addMessage(message);
                throw new DatawaveWebApplicationException(new IllegalArgumentException(message), errorReponse);
            }
        } else {
            matchingUuidType = null;
        }

        return matchingUuidType;
    }

    private class AllEventMockResponse extends DefaultEventQueryResponse {
        private static final long serialVersionUID = -4399127351489684829L;
        private final AbstractUUIDLookupCriteria criteria;

        public AllEventMockResponse(final AbstractUUIDLookupCriteria criteria) {
            this.criteria = criteria;
        }

        public AbstractUUIDLookupCriteria getLookupCriteria() {
            return this.criteria;
        }
    }
}
