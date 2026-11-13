package datawave.webservice.common.exception;

import datawave.webservice.result.BaseResponse;
import jakarta.ejb.ApplicationException;
import jakarta.ws.rs.core.Response;

@ApplicationException(rollback = true)
public class PreConditionFailedException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = -2711387596189941663L;

    public PreConditionFailedException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.PRECONDITION_FAILED.getStatusCode());
    }

}
