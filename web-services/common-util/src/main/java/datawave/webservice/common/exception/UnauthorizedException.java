package datawave.webservice.common.exception;

import jakarta.ejb.ApplicationException;
import jakarta.ws.rs.core.Response;

import datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class UnauthorizedException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = -6986449702126517288L;

    public UnauthorizedException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.UNAUTHORIZED.getStatusCode());
    }

}
