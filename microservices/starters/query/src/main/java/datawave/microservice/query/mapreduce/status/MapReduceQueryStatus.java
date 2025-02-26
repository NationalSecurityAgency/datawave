package datawave.microservice.query.mapreduce.status;

import static datawave.core.query.util.QueryUtil.PARAMETER_NAME_VALUE_SEPARATOR;
import static datawave.core.query.util.QueryUtil.PARAMETER_SEPARATOR;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.CANCELED;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.FAILED;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.RUNNING;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.SUBMITTED;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.SUCCEEDED;

import java.io.Serializable;
import java.util.Collections;

import org.apache.hadoop.mapreduce.JobStatus;
import org.springframework.util.MultiValueMap;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.results.mr.JobExecution;
import datawave.webservice.results.mr.MapReduceInfoResponse;

public class MapReduceQueryStatus implements Serializable {
    private static final long serialVersionUID = -8731070243293603952L;
    
    public enum MapReduceQueryState {
        DEFINED, SUBMITTED, RUNNING, SUCCEEDED, CLOSED, CANCELED, FAILED
    }
    
    private String id;
    
    private MapReduceQueryState state;
    
    private String jobName;
    
    private MultiValueMap<String,String> parameters;
    
    private DatawaveUserDetails currentUser;
    
    private Query query;
    
    private GenericQueryConfiguration config;
    
    private String jobId;
    
    private String jobTracker;
    
    private String hdfsUri;
    
    private String workingDirectory;
    
    private String resultsDirectory;
    
    private long lastUpdatedMillis;
    
    private DatawaveErrorCode errorCode;
    
    private String failureMessage;
    
    private String stackTrace;
    
    public MapReduceInfoResponse toMapReduceInfoResponse() {
        MapReduceInfoResponse resp = new MapReduceInfoResponse();
        resp.setId(id);
        resp.setWorkingDirectory(workingDirectory);
        resp.setResultsDirectory(resultsDirectory);
        resp.setRuntimeParameters(getStringRuntimeParameters());
        resp.setHdfs(hdfsUri);
        resp.setJobTracker(jobTracker);
        resp.setJobName(jobName);
        JobExecution jobExec = new JobExecution();
        jobExec.setMapReduceJobId(jobId);
        jobExec.setState(state.name());
        jobExec.setTimestamp(lastUpdatedMillis);
        resp.setJobExecutions(Collections.singletonList(jobExec));
        return resp;
    }
    
    private String getStringRuntimeParameters() {
        StringBuilder builder = new StringBuilder();
        for (String key : parameters.keySet()) {
            for (String value : parameters.get(key)) {
                builder.append(key).append(PARAMETER_NAME_VALUE_SEPARATOR).append(value).append(PARAMETER_SEPARATOR);
            }
        }
        return builder.toString();
    }
    
    public boolean isRunning() {
        return state == SUBMITTED || state == RUNNING;
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public MapReduceQueryState getState() {
        return state;
    }
    
    public void setState(MapReduceQueryState state) {
        this.state = state;
    }
    
    public void setState(JobStatus.State state) {
        switch (state) {
            case RUNNING:
                this.state = RUNNING;
                break;
            case SUCCEEDED:
                this.state = SUCCEEDED;
                break;
            case FAILED:
                this.state = FAILED;
                break;
            case PREP:
                this.state = SUBMITTED;
                break;
            case KILLED:
                this.state = CANCELED;
                break;
        }
    }
    
    public String getJobName() {
        return jobName;
    }
    
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
    
    public MultiValueMap<String,String> getParameters() {
        return parameters;
    }
    
    public void setParameters(MultiValueMap<String,String> parameters) {
        this.parameters = parameters;
    }
    
    public DatawaveUserDetails getCurrentUser() {
        return currentUser;
    }
    
    public void setCurrentUser(DatawaveUserDetails currentUser) {
        this.currentUser = currentUser;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public GenericQueryConfiguration getConfig() {
        return config;
    }
    
    public void setConfig(GenericQueryConfiguration config) {
        this.config = config;
    }
    
    public String getJobId() {
        return jobId;
    }
    
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }
    
    public String getJobTracker() {
        return jobTracker;
    }
    
    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }
    
    public String getHdfsUri() {
        return hdfsUri;
    }
    
    public void setHdfsUri(String hdfsUri) {
        this.hdfsUri = hdfsUri;
    }
    
    public String getWorkingDirectory() {
        return workingDirectory;
    }
    
    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }
    
    public String getResultsDirectory() {
        return resultsDirectory;
    }
    
    public void setResultsDirectory(String resultsDirectory) {
        this.resultsDirectory = resultsDirectory;
    }
    
    public long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }
    
    public void setLastUpdatedMillis(long lastUpdatedMillis) {
        this.lastUpdatedMillis = lastUpdatedMillis;
    }
    
    public DatawaveErrorCode getErrorCode() {
        return errorCode;
    }
    
    public void setErrorCode(DatawaveErrorCode errorCode) {
        this.errorCode = errorCode;
    }
    
    public String getFailureMessage() {
        return failureMessage;
    }
    
    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
    
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
}
