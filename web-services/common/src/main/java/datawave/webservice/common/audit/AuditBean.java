package datawave.webservice.common.audit;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;
import jakarta.annotation.security.RunAs;
import jakarta.ejb.LocalBean;
import jakarta.ejb.Stateless;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MultivaluedMap;

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
