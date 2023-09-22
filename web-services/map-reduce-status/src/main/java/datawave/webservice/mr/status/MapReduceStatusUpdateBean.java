package datawave.webservice.mr.status;

import static datawave.webservice.mr.state.MapReduceStatePersisterBean.MapReduceState;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;

import datawave.annotation.Required;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.mr.state.MapReduceStatePersisterBean;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;

@Path("/MapReduceStatus")
@RunAs("InternalUser")
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class MapReduceStatusUpdateBean {

    private Logger log = Logger.getLogger(this.getClass());

    @Inject
    private MapReduceStatePersisterBean mapReduceState;

    /**
     * This method is meant to be a callback from the Hadoop infrastructure and is not protected. When a BulkResults job is submitted the
     * "job.end.notification.url" property is set to public URL endpoint for this servlet. The Hadoop infrastructure will call back to this servlet. If the call
     * back fails for some reason, then Hadoop will retry for the number of configured attempts (job.end.retry.attempts) at some configured interval
     * (job.end.retry.interval)
     *
     * @param jobId
     *            the job id
     * @param jobStatus
     *            the job status
     *
     * @HTTP 200 success
     * @HTTP 500 failure
     *
     * @return datawave.webservice.result.VoidResponse
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Path("/updateState")
    @PermitAll
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    public VoidResponse updateState(@Required("jobId") @QueryParam("jobId") String jobId, @Required("jobStatus") @QueryParam("jobStatus") String jobStatus) {
        log.info("Received MapReduce status update for job: " + jobId + ", new status: " + jobStatus);

        VoidResponse response = new VoidResponse();
        try {
            mapReduceState.updateState(jobId, MapReduceState.valueOf(jobStatus));
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MAPRED_UPDATE_STATUS_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        }
    }

}
