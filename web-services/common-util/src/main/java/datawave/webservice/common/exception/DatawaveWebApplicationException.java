package datawave.webservice.common.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import datawave.webservice.result.BaseResponse;

@ApplicationException(inherited = true, rollback = true)
public class DatawaveWebApplicationException extends WebApplicationException {
    
    private BaseResponse datawaveBaseResponse = null;
    
    private static final long serialVersionUID = 1L;
    
    public DatawaveWebApplicationException(Throwable t, BaseResponse response) {
        super(t, Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build());
        datawaveBaseResponse = response;
    }
    
    public DatawaveWebApplicationException(Throwable t, BaseResponse response, int code) {
        super(t, Response.status(code).entity(response).build());
        datawaveBaseResponse = response;
    }
    
    public BaseResponse getDatawaveBaseResponse() {
        return datawaveBaseResponse;
    }
    
}
