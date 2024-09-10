package datawave.webservice.common.exception;

import javax.ejb.ApplicationException;
import javax.ws.rs.core.Response;

@ApplicationException(rollback = true)
public class NoResultsException extends DatawaveWebApplicationException {

    private static final long serialVersionUID = 1L;

    private String id;
    private long startTime;
    private long endTime;

    public NoResultsException(Throwable t) {
        super(t, null, Response.Status.NO_CONTENT.getStatusCode());
    }

    public NoResultsException(Throwable t, String id) {
        this(t);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}
