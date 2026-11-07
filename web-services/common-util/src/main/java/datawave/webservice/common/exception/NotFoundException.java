package datawave.webservice.common.exception;

import jakarta.ejb.ApplicationException;
import jakarta.ws.rs.core.Response;

import datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class NotFoundException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = -3719841094840816728L;

    public NotFoundException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.NOT_FOUND.getStatusCode());
    }

}
