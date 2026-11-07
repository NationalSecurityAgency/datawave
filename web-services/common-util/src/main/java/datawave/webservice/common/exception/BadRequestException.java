package datawave.webservice.common.exception;

import jakarta.ejb.ApplicationException;
import jakarta.ws.rs.core.Response;

import datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class BadRequestException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = -7271357906508218342L;

    public BadRequestException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.BAD_REQUEST.getStatusCode());
    }

}
