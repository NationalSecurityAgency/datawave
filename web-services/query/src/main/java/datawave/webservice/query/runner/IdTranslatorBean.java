package datawave.webservice.query.runner;

import java.security.Principal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.time.DateUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.query.data.UUIDType;
import datawave.resteasy.util.DateFormatter;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.AuthorizationsUtil;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.configuration.IdTranslatorConfiguration;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.VoidResponse;

@Path("/Query")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class IdTranslatorBean {

    @Inject
    private QueryExecutorBean queryExecutor;

    @Inject
    @SpringBean(refreshable = true)
    private IdTranslatorConfiguration idTranslatorConfiguration;

    @Resource
    private EJBContext ctx;

    private Logger log = Logger.getLogger(this.getClass());

    private String columnVisibility = null;
    private Date beginDate = null;
    private static final String ID_TRANS_LOGIC = "IdTranslationQuery";
    private static final String ID_TRANS_TLD_LOGIC = "IdTranslationTLDQuery";

    @Inject
    private QueryLogicFactory queryLogicFactory;

    private Map<String,UUIDType> uuidTypes = Collections.synchronizedMap(new HashMap<>());

    @PostConstruct
    public void init() {
        this.columnVisibility = idTranslatorConfiguration.getColumnVisibility();

        // Populate the UUIDType map
        final List<UUIDType> types = idTranslatorConfiguration.getUuidTypes();
        this.uuidTypes.clear();
        if (null != types) {
            for (final UUIDType type : types) {
                if (null != type) {
                    this.uuidTypes.put(type.getFieldName().toUpperCase(), type);
                }
            }
        }

        // Assign the begin date
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        try {
            this.beginDate = sdf.parse(idTranslatorConfiguration.getBeginDate());
        } catch (ParseException e) {
            this.log.error(e.getMessage(), e);
        }
    }

    /**
     * Get one or more ID(s), if any, that correspond to the given ID. This method only returns the first page, so set pagesize appropriately. Since the
     * underlying query is automatically closed, callers are NOT expected to invoke the <b>/{id}/next</b> or <b>/{id}/close</b> endpoints.
     *
     * @param id
     *            - the ID for which to find related IDs (@Required)
     * @param pagesize
     *            - optional pagesize (default 100)
     * @param pageTimeout
     *            - optional pageTimeout (default -1)
     * @param systemFrom
     *            name of the sending system
     * @param dataSetType
     *            (@Required)
     * @param purpose
     *            (@Required)
     * @param parentAuditId
     *            optional
     * @param TLDonly
     *            TLDonly
     * @return event results - {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/translateId/{id}")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public BaseQueryResponse translateId(@PathParam("id") String id, @QueryParam("pagesize") @DefaultValue("100") int pagesize,
                    @QueryParam("pageTimeout") @DefaultValue("-1") int pageTimeout, @QueryParam("systemFrom") String systemFrom,
                    @QueryParam("dataSetType") String dataSetType, @QueryParam("purpose") String purpose, @QueryParam("parentAuditId") String parentAuditId,
                    @QueryParam("TLDonly") @DefaultValue("true") String TLDonly) {

        String queryId = null;
        BaseQueryResponse response;
        try {
            response = submitTranslationQuery(id, pagesize, pageTimeout, TLDonly);
            queryId = response.getQueryId();
            return response;
        } finally {
            if (null != queryId) {
                this.queryExecutor.close(queryId);
            }
        }
    }

    /**
     * Get the ID(s), if any, associated with the specified ID list. Because this endpoint may return multiple pages, callers are expected to invoke the
     * <b>/{id}/next</b> endpoint until receiving an HTTP 204 status, and then invoke the <b>/{id}/close</b> endpoint. Failure to invoke such endpoints prevents
     * the system from cleaning up and releasing valuable resources in an efficient manner.
     *
     * @param idList
     *            - Comma separated list of IDs for which to return related IDs.
     *            "13383f57-45dc-4709-934a-363117e7c473,6ea02cb3-644c-4c2e-9739-76322dfb477b"(@Required)
     * @param pagesize
     *            - optional pagesize (default 100)
     * @param TLDonly
     *            TLDonly
     * @param pageTimeout
     *            page timeout
     * @return event results - {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @POST
    @Path("/translateIDs")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public BaseQueryResponse translateIDs(@FormParam("idList") String idList, @FormParam("pagesize") @DefaultValue("100") int pagesize,
                    @FormParam("pageTimeout") @DefaultValue("-1") int pageTimeout, @FormParam("TLDonly") @DefaultValue("true") String TLDonly) {

        return submitTranslationQuery(idList, pagesize, pageTimeout, TLDonly);
    }

    private String buildQuery(String ids) {
        StringBuilder query = new StringBuilder();

        String idArray[] = ids.split(",");
        for (String id : idArray) {
            // Build the query by explicitly searching for all the configured ID types
            for (String uuidType : uuidTypes.keySet()) {
                if (query.length() > 0) {
                    query.append(" OR ");
                }
                query.append(uuidType.toUpperCase() + ":\"" + id + "\"");
            }
        }
        return query.toString();
    }

    private BaseQueryResponse submitTranslationQuery(String ids, int pagesize, int pageTimeout, String TLDonly) {

        String queryName = UUID.randomUUID().toString();
        String parameters = "query.syntax:LUCENE";
        Date endDate = DateUtils.addDays(new Date(), 2);
        Date expirationDate = new Date(endDate.getTime() + 1000 * 60 * 60);

        String query = buildQuery(ids);

        String logicName = null;
        if (TLDonly.equalsIgnoreCase("true")) {
            logicName = ID_TRANS_TLD_LOGIC;
        } else {
            logicName = ID_TRANS_LOGIC;
        }

        // Use the user's full authorizations when creating the query. Note that this will be ignored by the query logic, but we need
        // to pass a valid subset of the user's auths to even create the query.
        String auths = getAuths(logicName, ctx.getCallerPrincipal());

        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putAll(idTranslatorConfiguration.optionalParamsToMap());
        p.putSingle(QueryParameters.QUERY_LOGIC_NAME, logicName);
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_VISIBILITY, this.columnVisibility);
        SimpleDateFormat formatter = new SimpleDateFormat(DateFormatter.getFormatPattern());
        if (beginDate != null) {
            p.putSingle(QueryParameters.QUERY_BEGIN, formatter.format(beginDate));
        }
        if (endDate != null) {
            p.putSingle(QueryParameters.QUERY_END, formatter.format(endDate));
        }
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, auths);
        p.putSingle(QueryParameters.QUERY_EXPIRATION, formatter.format(expirationDate));
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_PAGETIMEOUT, Integer.toString(pageTimeout));
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, QueryPersistence.TRANSIENT.name());
        p.putSingle(QueryParameters.QUERY_TRACE, Boolean.toString(false));
        // Put the original parameter string into the map also
        p.putSingle(QueryParameters.QUERY_PARAMS, parameters);

        return queryExecutor.createQueryAndNext(logicName, p);
    }

    private String getAuths(String logicName, Principal principal) {
        String userAuths;
        try {
            QueryLogic<?> logic = queryLogicFactory.getQueryLogic(logicName, principal);
            // the query principal is our local principal unless the query logic has a different user operations
            DatawavePrincipal queryPrincipal = (logic.getUserOperations() == null) ? (DatawavePrincipal) principal
                            : logic.getUserOperations().getRemoteUser((DatawavePrincipal) principal);
            userAuths = AuthorizationsUtil.buildUserAuthorizationString(queryPrincipal);
        } catch (Exception e) {
            log.error("Failed to get user query authorizations", e);
            throw new DatawaveWebApplicationException(e, new VoidResponse());
        }
        return userAuths;
    }

}
