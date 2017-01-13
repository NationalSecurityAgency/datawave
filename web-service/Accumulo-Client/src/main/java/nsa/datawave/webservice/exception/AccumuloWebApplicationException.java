package nsa.datawave.webservice.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import nsa.datawave.webservice.response.BaseResponse;

@ApplicationException(inherited = true, rollback = true)
public class AccumuloWebApplicationException extends WebApplicationException {
    
    private static final long serialVersionUID = 1L;
    
    public AccumuloWebApplicationException(Throwable t, BaseResponse response) {
        super(t, Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build());
    }
    
    public AccumuloWebApplicationException(Throwable t, BaseResponse response, int code) {
        super(t, Response.status(code).entity(response).build());
    }
    
}
