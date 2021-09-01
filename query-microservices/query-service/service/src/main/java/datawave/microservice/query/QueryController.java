package datawave.microservice.query;

import com.codahale.metrics.annotation.Timed;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {
    private final QueryManagementService queryManagementService;
    
    public QueryController(QueryManagementService queryManagementService) {
        this.queryManagementService = queryManagementService;
    }
    
    @Timed(name = "dw.query.listQueryLogic", absolute = true)
    @RequestMapping(path = "listQueryLogic", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public QueryLogicResponse listQueryLogic(@AuthenticationPrincipal ProxiedUserDetails currentUser) {
        return queryManagementService.listQueryLogic(currentUser);
    }
    
    @Timed(name = "dw.query.defineQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/define", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> define(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.define(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.createQuery", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE)
    @RequestMapping(path = "{queryLogic}/create", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> create(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.create(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.createAndNext", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.CREATE_AND_NEXT)
    @RequestMapping(path = "{queryLogic}/createAndNext", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse createQueryAndNext(@PathVariable String queryLogic, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.createAndNext(queryLogic, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.next", absolute = true)
    @EnrichQueryMetrics(methodType = EnrichQueryMetrics.MethodType.NEXT)
    @RequestMapping(path = "{queryId}/next", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public BaseQueryResponse next(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.next(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.cancel", absolute = true)
    @RequestMapping(path = "{queryId}/cancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse cancel(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.cancel(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminCancel", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminCancel", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancel(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancel(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.close", absolute = true)
    @RequestMapping(path = "{queryId}/close", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse close(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.close(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminClose", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminClose", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml",
            "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminClose(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminClose(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.reset", absolute = true)
    @RequestMapping(path = "{queryId}/reset", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> reset(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.reset(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.remove", absolute = true)
    @RequestMapping(path = "{queryId}/remove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse remove(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.remove(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.adminRemove", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "{queryId}/adminRemove", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemove(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemove(queryId, currentUser);
    }
    
    @Timed(name = "dw.query.update", absolute = true)
    @RequestMapping(path = "{queryId}/update", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> update(@PathVariable String queryId, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.update(queryId, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.duplicate", absolute = true)
    @RequestMapping(path = "{queryId}/duplicate", method = {RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public GenericResponse<String> duplicate(@PathVariable String queryId, @RequestParam MultiValueMap<String,String> parameters,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.duplicate(queryId, parameters, currentUser);
    }
    
    @Timed(name = "dw.query.list", absolute = true)
    @RequestMapping(path = "list", method = {RequestMethod.GET}, produces = {"text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml",
            "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse list(@RequestParam(required = false) String queryId, @RequestParam(required = false) String queryName,
                    @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, queryName, currentUser);
    }
    
    @Timed(name = "dw.query.adminList", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminList", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse adminList(@RequestParam(required = false) String queryId, @RequestParam(required = false) String user,
                    @RequestParam(required = false) String queryName, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminList(queryId, queryName, user, currentUser);
    }
    
    @Timed(name = "dw.query.get", absolute = true)
    @RequestMapping(path = "{queryId}", method = {RequestMethod.GET}, produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml",
            "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public QueryImplListResponse get(@PathVariable String queryId, @AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.list(queryId, null, currentUser);
    }
    
    @Timed(name = "dw.query.adminCancelAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCancelAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCancelAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCancelAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminCloseAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminCloseAll", method = {RequestMethod.PUT, RequestMethod.POST}, produces = {"application/xml", "text/xml", "application/json",
            "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminCloseAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminCloseAll(currentUser);
    }
    
    @Timed(name = "dw.query.adminRemoveAll", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "adminRemoveAll", method = {RequestMethod.DELETE}, produces = {"application/xml", "text/xml", "application/json", "text/yaml",
            "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
    public VoidResponse adminRemoveAll(@AuthenticationPrincipal ProxiedUserDetails currentUser) throws QueryException {
        return queryManagementService.adminRemoveAll(currentUser);
    }
}
