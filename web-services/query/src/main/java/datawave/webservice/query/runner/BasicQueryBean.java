package datawave.webservice.query.runner;

import com.codahale.metrics.annotation.Timed;
import datawave.annotation.GenerateQuerySessionId;
import datawave.annotation.Required;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.resteasy.interceptor.CreateQuerySessionIDFilter;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.logic.QueryLogicDescription;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryWizardResultResponse;
import datawave.webservice.result.QueryWizardStep1Response;
import datawave.webservice.result.QueryWizardStep2Response;
import datawave.webservice.result.QueryWizardStep3Response;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.SessionContext;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/BasicQuery")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class BasicQueryBean {

    private static final String PRIVILEGED_USER = "PrivilegedUser";

    /**
     * Used when getting a plan prior to creating a query
     */
    public static final String EXPAND_VALUES = "expand.values";
    public static final String EXPAND_FIELDS = "expand.fields";

    static final List<String> NO_PLAN_REQUIRED = Arrays.asList("datawave.query.tables.content.ContentQueryTable");

    private final Logger log = Logger.getLogger(BasicQueryBean.class);

    @Inject
    private QueryLogicFactory queryLogicFactory;

    @Inject
    private QueryExecutorBean queryExecutor;

    @Inject
    @ConfigProperty(name = "dw.cdn.jquery.uri", defaultValue = "/jquery.min.js")
    private String jqueryUri;

    @Inject
    @ConfigProperty(name = "dw.cdn.dataTables.uri", defaultValue = "/jquery.dataTables.min.js")
    private String dataTablesUri;

    @Resource
    private EJBContext ctx;

    @Resource
    private SessionContext sessionContext;

    @Inject
    private ResponseObjectFactory responseObjectFactory;

    @PostConstruct
    public void init() {

    }

    @PreDestroy
    public void close() {

    }

    /**
     * Display the first step for a simple query web UI in the quickstart
     *
     * @HTTP 200 Success
     * @return datawave.webservice.result.QueryWizardStep1Response
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @Path("/showQueryWizard")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @GET
    @Interceptors({ResponseInterceptor.class})
    @Timed(name = "dw.query.showQueryWizard", absolute = true)
    public QueryWizardStep1Response showQueryWizardStep1() {
        QueryWizardStep1Response response = new QueryWizardStep1Response();
        List<QueryLogic<?>> logicList = queryLogicFactory.getQueryLogicList();
        List<QueryLogicDescription> logicConfigurationList = new ArrayList<>();

        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = responseObjectFactory.getQueryImpl();
        Date now = new Date();
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");

        for (QueryLogic<?> l : logicList) {
            try {
                QueryLogicDescription d = new QueryLogicDescription(l.getLogicName());
                d.setAuditType(l.getAuditType(null).toString());
                d.setLogicDescription(l.getLogicDescription());

                Set<String> optionalQueryParameters = l.getOptionalQueryParameters();
                if (optionalQueryParameters != null) {
                    d.setSupportedParams(new ArrayList<>(optionalQueryParameters));
                }
                Set<String> requiredQueryParameters = l.getRequiredQueryParameters();
                if (requiredQueryParameters != null) {
                    d.setRequiredParams(new ArrayList<>(requiredQueryParameters));
                }
                Set<String> exampleQueries = l.getExampleQueries();
                if (exampleQueries != null) {
                    d.setExampleQueries(new ArrayList<>(exampleQueries));
                }
                Set<String> requiredRoles = l.getRequiredRoles();
                if (requiredRoles != null) {
                    List<String> requiredRolesList = new ArrayList<>();
                    requiredRolesList.addAll(l.getRequiredRoles());
                    d.setRequiredRoles(requiredRolesList);
                }

                try {
                    d.setResponseClass(l.getResponseClass(q));
                } catch (QueryException e) {
                    log.error(e, e);
                    response.addException(e);
                    d.setResponseClass("unknown");
                }

                List<String> querySyntax = new ArrayList<>();
                try {
                    Method m = l.getClass().getMethod("getQuerySyntaxParsers");
                    Object result = m.invoke(l);
                    if (result instanceof Map<?,?>) {
                        Map<?,?> map = (Map<?,?>) result;
                        for (Object o : map.keySet())
                            querySyntax.add(o.toString());
                    }
                } catch (Exception e) {
                    log.warn("Unable to get query syntax for query logic: " + l.getClass().getCanonicalName());
                }
                if (querySyntax.isEmpty()) {
                    querySyntax.add("CUSTOM");
                }
                d.setQuerySyntax(querySyntax);

                logicConfigurationList.add(d);
            } catch (Exception e) {
                log.error("Error setting query logic description", e);
            }
        }
        Collections.sort(logicConfigurationList, Comparator.comparing(QueryLogicDescription::getName));
        response.setQueryLogicList(logicConfigurationList);

        return response;
    }

    /**
     * Display the second step for a simple query web UI in the quickstart
     *
     * @param queryParameters
     *            parameters
     * @param httpHeaders
     *            headers
     * @HTTP 200 Success
     * @return datawave.webservice.result.QueryWizardStep2Response
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @Path("/showQueryWizardStep2")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @POST
    @Interceptors({ResponseInterceptor.class})
    @Timed(name = "dw.query.showQueryWizardStep2", absolute = true)
    public QueryWizardStep2Response showQueryWizardStep2(MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders) {
        QueryWizardStep2Response response = new QueryWizardStep2Response();
        String queryType = queryParameters.getFirst("queryType");
        QueryLogicDescription theQld = null;
        List<QueryLogic<?>> logicList = queryLogicFactory.getQueryLogicList();

        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = responseObjectFactory.getQueryImpl();
        Date now = new Date();
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");

        UserOperations userService = null;

        for (QueryLogic<?> l : logicList) {
            try {

                if (l.getLogicName().equals(queryType)) {

                    QueryLogicDescription d = new QueryLogicDescription(l.getLogicName());
                    d.setAuditType(l.getAuditType(null).toString());
                    d.setLogicDescription(l.getLogicDescription());
                    userService = l.getUserOperations();
                    theQld = d;

                    Set<String> optionalQueryParameters = l.getOptionalQueryParameters();
                    if (optionalQueryParameters != null) {
                        d.setSupportedParams(new ArrayList<>(optionalQueryParameters));
                    }
                    Set<String> requiredQueryParameters = l.getRequiredQueryParameters();
                    if (requiredQueryParameters != null) {
                        d.setRequiredParams(new ArrayList<>(requiredQueryParameters));
                    }
                    Set<String> exampleQueries = l.getExampleQueries();
                    if (exampleQueries != null) {
                        d.setExampleQueries(new ArrayList<>(exampleQueries));
                    }
                    Set<String> requiredRoles = l.getRequiredRoles();
                    if (requiredRoles != null) {
                        List<String> requiredRolesList = new ArrayList<>();
                        requiredRolesList.addAll(l.getRequiredRoles());
                        d.setRequiredRoles(requiredRolesList);
                    }

                    try {
                        d.setResponseClass(l.getResponseClass(q));
                    } catch (QueryException e) {
                        log.error(e, e);
                        response.addException(e);
                        d.setResponseClass("unknown");
                    }

                    List<String> querySyntax = new ArrayList<>();
                    try {
                        Method m = l.getClass().getMethod("getQuerySyntaxParsers");
                        Object result = m.invoke(l);
                        if (result instanceof Map<?,?>) {
                            Map<?,?> map = (Map<?,?>) result;
                            for (Object o : map.keySet())
                                querySyntax.add(o.toString());
                        }
                    } catch (Exception e) {
                        log.warn("Unable to get query syntax for query logic: " + l.getClass().getCanonicalName());
                    }
                    if (querySyntax.isEmpty()) {
                        querySyntax.add("CUSTOM");
                    }
                    d.setQuerySyntax(querySyntax);

                    break;

                }
            } catch (Exception e) {
                log.error("Error setting query logic description", e);
                throw new RuntimeException(e);
            }
        }

        try {
            DatawavePrincipal queryPrincipal = (DatawavePrincipal) ((userService == null) ? ctx.getCallerPrincipal()
                            : userService.getRemoteUser((DatawavePrincipal) ctx.getCallerPrincipal()));
            response.setAuthString(WSAuthorizationsUtil.buildUserAuthorizationString(queryPrincipal));
        } catch (AuthorizationException e) {
            throw new RuntimeException(e);
        }
        response.setTheQueryLogicDescription(theQld);

        return response;
    }

    /**
     * Display the query plan and link to basic query results for a simple query web UI in the quickstart
     *
     * @param queryParameters
     *            parameters
     * @param httpHeaders
     *            headers
     * @param logicName
     *            logic name
     * @HTTP 200 Success
     * @return datawave.webservice.result.QueryWizardStep3Response
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @Path("/{logicName}/showQueryWizardStep3")
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/BasicQuery/")
    @Interceptors({ResponseInterceptor.class})
    @Timed(name = "dw.query.showQueryWizardStep3", absolute = true)
    public QueryWizardStep3Response showQueryWizardStep3(@Required("logicName") @PathParam("logicName") String logicName,
                    MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        GenericResponse<String> createResponse;
        QueryWizardStep3Response queryWizardStep3Response = new QueryWizardStep3Response();
        try {
            createResponse = queryExecutor.createQuery(logicName, queryParameters, httpHeaders);
        } catch (Exception e) {
            queryWizardStep3Response.setErrorMessage(e.getMessage());
            return queryWizardStep3Response;
        }
        String queryId = createResponse.getResult();
        CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
        queryWizardStep3Response.setQueryId(queryId);

        BaseQueryLogic logic = getQueryLogic(logicName);
        if (logic != null && !(NO_PLAN_REQUIRED.contains(logic.getClass().getName()))) {
            GenericResponse<String> planResponse;
            try {
                planResponse = queryExecutor.plan(queryId);
            } catch (Exception e) {
                queryWizardStep3Response.setErrorMessage(e.getMessage());
                return queryWizardStep3Response;
            }

            queryWizardStep3Response.setQueryPlan(planResponse.getResult());
        } else
            queryWizardStep3Response.setQueryPlan("No plan required for this query");

        return queryWizardStep3Response;
    }

    /**
     * Gets the next page of results from the query object. If the object is no longer alive, meaning that the current session has expired, then this fail. The
     * response object type is dynamic, see the listQueryLogic operation to determine what the response type object will be.
     *
     * @param id
     *            - (@Required)
     * @see datawave.webservice.query.runner.BasicQueryBean#showQueryWizardResults(String) for the @Required definition
     *
     * @return datawave.webservice.result.BaseQueryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-query-page-number page number returned by this call
     * @ResponseHeader X-query-last-page if true then there are no more pages for this query, caller should call close()
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 404 if id not found
     * @HTTP 412 if the query is no longer alive, client should call #reset(String) and try again
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{id}/showQueryWizardResults")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Timed(name = "dw.query.showQueryWizardResults", absolute = true)
    public QueryWizardResultResponse showQueryWizardResults(@Required("id") @PathParam("id") String id) {

        QueryWizardResultResponse theResponse = new QueryWizardResultResponse(jqueryUri, dataTablesUri);
        theResponse.setQueryId(id);
        BaseQueryResponse theNextResults;
        try {
            theNextResults = queryExecutor.next(id);
        } catch (Exception e) {
            theNextResults = null;
        }
        theResponse.setResponse(theNextResults);
        return theResponse;
    }

    private BaseQueryLogic getQueryLogic(String logicName) {
        BaseQueryLogic theLogic = null;

        List<QueryLogic<?>> logicList = queryLogicFactory.getQueryLogicList();

        for (QueryLogic<?> l : logicList) {
            try {
                if (l.getLogicName().equals(logicName)) {
                    return (BaseQueryLogic) l;
                }
            } catch (Exception e) {
                log.error("Error getting query logic name", e);
            }
        }

        return theLogic;
    }

}
