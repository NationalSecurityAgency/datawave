package datawave.webservice.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.core.Response;

import datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class BadRequestException extends AccumuloWebApplicationException {
    
    private static final long serialVersionUID = 1L;
    
    public BadRequestException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.BAD_REQUEST.getStatusCode());
    }
    
}
