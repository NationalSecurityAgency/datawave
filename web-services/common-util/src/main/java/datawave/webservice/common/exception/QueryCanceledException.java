package datawave.webservice.common.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.core.Response;

import datawave.webservice.result.BaseResponse;

@ApplicationException(rollback = true)
public class QueryCanceledException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = 1L;

    public QueryCanceledException(Throwable t, BaseResponse response) {
        super(t, response, Response.Status.NO_CONTENT.getStatusCode());
    }

}
