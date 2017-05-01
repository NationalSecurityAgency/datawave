package nsa.datawave.webservice.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.core.Response;

import nsa.datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class UnauthorizedException extends AccumuloWebApplicationException {
    
    private static final long serialVersionUID = 1L;
    
    public UnauthorizedException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.UNAUTHORIZED.getStatusCode());
    }
    
}
