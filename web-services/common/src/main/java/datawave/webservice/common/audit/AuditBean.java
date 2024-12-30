package datawave.webservice.common.audit;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.springframework.util.MultiValueMap;

import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;

@Path("/Common/Auditor")
@LocalBean
@Stateless
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
public class AuditBean {
    private static final Logger log = Logger.getLogger(AuditBean.class);

    @Inject
    private AuditService auditService;

    @Inject
    private AuditParameterBuilder auditParameterBuilder;

    @POST
    @Path("/audit")
    @Consumes("*/*")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    public VoidResponse auditRest(MultivaluedMap<String,String> parameters) {
        VoidResponse response = new VoidResponse();
        try {
            auditService.audit(auditParameterBuilder.validate(parameters));
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.AUDITING_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }

    public String audit(MultiValueMap<String,String> parameters) throws Exception {
        return auditService.audit(auditParameterBuilder.convertAndValidate(parameters));
    }
}
