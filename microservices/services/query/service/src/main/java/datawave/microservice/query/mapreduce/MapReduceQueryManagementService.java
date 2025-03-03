package datawave.microservice.query.mapreduce;

import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.JOB_NAME;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.PARAMETERS;
import static datawave.microservice.query.mapreduce.config.MapReduceQueryProperties.QUERY_ID;
import static datawave.microservice.query.mapreduce.jobs.OozieJob.WORKFLOW;
import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.OOZIE_SUBMIT;
import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.SUBMIT;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.CANCELED;
import static datawave.microservice.query.mapreduce.status.MapReduceQueryStatus.MapReduceQueryState.FAILED;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.DEFINE;

import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteMapReduceQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.mapreduce.config.MapReduceJobProperties;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.mapreduce.jobs.MapReduceJob;
import datawave.microservice.query.mapreduce.jobs.OozieJob;
import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest;
import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequestHandler;
import datawave.microservice.query.mapreduce.status.MapReduceQueryCache;
import datawave.microservice.query.mapreduce.status.MapReduceQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import datawave.webservice.results.mr.MapReduceInfoResponse;
import datawave.webservice.results.mr.MapReduceInfoResponseList;
import datawave.webservice.results.mr.MapReduceJobDescription;
import datawave.webservice.results.mr.MapReduceJobDescriptionList;
import datawave.webservice.results.mr.ResultFile;

@Service
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryManagementService implements MapReduceQueryRequestHandler {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final String PARAMETER_SEPARATOR = ";";
    private static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    private final QueryProperties queryProperties;
    
    private final MapReduceQueryProperties mapReduceQueryProperties;
    
    private final ApplicationEventPublisher eventPublisher;
    
    private final BusProperties busProperties;
    
    private final QueryManagementService queryManagementService;
    
    private final MapReduceQueryCache mapReduceQueryCache;
    
    private final Map<String,Supplier<MapReduceJob>> mapReduceJobs;
    
    private final Configuration configuration;
    
    private final Map<String,CountDownLatch> queryLatchMap = new ConcurrentHashMap<>();
    
    public MapReduceQueryManagementService(QueryProperties queryProperties, MapReduceQueryProperties mapReduceQueryProperties,
                    ApplicationEventPublisher eventPublisher, BusProperties busProperties, QueryManagementService queryManagementService,
                    MapReduceQueryCache mapReduceQueryCache, Map<String,Supplier<MapReduceJob>> mapReduceJobs) {
        this.queryProperties = queryProperties;
        this.mapReduceQueryProperties = mapReduceQueryProperties;
        this.eventPublisher = eventPublisher;
        this.busProperties = busProperties;
        this.queryManagementService = queryManagementService;
        this.mapReduceQueryCache = mapReduceQueryCache;
        this.mapReduceJobs = mapReduceJobs;
        this.configuration = new Configuration();
        if (mapReduceQueryProperties.getFsConfigResources() != null) {
            for (String resource : mapReduceQueryProperties.getFsConfigResources()) {
                this.configuration.addResource(new Path(resource));
            }
        }
    }
    
    public MapReduceJobDescriptionList listConfigurations(String jobType, DatawaveUserDetails currentUser) {
        log.info("Request: listConfigurations from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), jobType);
        
        MapReduceJobDescriptionList response = new MapReduceJobDescriptionList();
        List<MapReduceJobDescription> jobs = new ArrayList<>();
        if (jobType.equals("none")) {
            jobType = null;
        }
        for (Map.Entry<String,MapReduceJobProperties> entry : mapReduceQueryProperties.getJobs().entrySet()) {
            if (jobType != null && !entry.getValue().getJobType().equals(jobType)) {
                continue;
            }
            jobs.add(createMapReduceJobDescription(entry.getKey(), entry.getValue()));
        }
        response.setResults(jobs);
        return response;
    }
    
    protected MapReduceJobDescription createMapReduceJobDescription(String name, MapReduceJobProperties jobProperties) {
        MapReduceJobDescription desc = new MapReduceJobDescription();
        desc.setName(name);
        desc.setJobType(jobProperties.getJobType());
        desc.setDescription(jobProperties.getDescription());
        List<String> required = new ArrayList<>(jobProperties.getRequiredRuntimeParameters().keySet());
        desc.setRequiredRuntimeParameters(required);
        List<String> optional = new ArrayList<>(jobProperties.getOptionalRuntimeParameters().keySet());
        desc.setOptionalRuntimeParameters(optional);
        return desc;
    }
    
    public GenericResponse<String> oozieSubmit(MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser) throws QueryException {
        
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: submit from {} with params: {}", user, parameters);
        } else {
            log.info("Request: submit from {}", user);
        }
        
        try {
            MultiValueMap<String,String> parsedParameters = parseParameters(parameters);
            
            String workflow = parsedParameters.getFirst(WORKFLOW);
            
            String id = submitOozieWorkflow(workflow, parsedParameters, currentUser);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(id);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error submitting oozie workflow", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error submitting oozie workflow.");
        }
    }
    
    public GenericResponse<String> submit(MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser) throws QueryException {
        String user = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName());
        if (log.isDebugEnabled()) {
            log.info("Request: submit from {} with params: {}", user, parameters);
        } else {
            log.info("Request: submit from {}", user);
        }
        
        try {
            MultiValueMap<String,String> parsedParameters = parseParameters(parameters);
            
            String jobName = parsedParameters.getFirst(JOB_NAME);
            
            // make sure the query is valid, and the user can act on it
            QueryStatus queryStatus = queryManagementService.validateRequest(parsedParameters.getFirst(QUERY_ID), currentUser);
            
            // make sure the state is define
            if (queryStatus.getQueryState() == DEFINE) {
                String id = submitJob(jobName, parsedParameters, queryStatus, currentUser);
                GenericResponse<String> response = new GenericResponse<>();
                response.setResult(id);
                return response;
            } else {
                throw new BadRequestQueryException("Submit can only be called on a defined query", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error submitting job", e);
            throw new QueryException(DatawaveErrorCode.QUERY_SETUP_ERROR, e, "Unknown error submitting job.");
        }
    }
    
    protected String submitJob(String jobName, MultiValueMap<String,String> parameters, QueryStatus queryStatus, DatawaveUserDetails currentUser)
                    throws Exception {
        
        // validate the job
        MapReduceJob mapReduceJob = validateJob(jobName, parameters, currentUser);
        
        // create the audit parameters from the query definition
        MultiValueMap<String,String> auditParameters = new LinkedMultiValueMap<>(parameters);
        auditParameters.addAll(new LinkedMultiValueMap<>(queryStatus.getQuery().toMap()));
        
        // validate the query and get the query logic
        QueryLogic<?> queryLogic = queryManagementService.validateQuery(queryStatus.getQuery().getQueryLogicName(), auditParameters, currentUser);
        
        String id = mapReduceJob.createId(currentUser);
        
        // audit the job
        auditParameters.add(AuditParameters.AUDIT_ID, id);
        queryManagementService.audit(queryStatus.getQuery(), queryLogic, auditParameters, currentUser);
        
        // store the job in the cache
        mapReduceQueryCache.createQuery(id, jobName, parameters, queryStatus.getQuery(), currentUser);
        
        // send the job off to the mapreduce service
        sendRequestAwaitResponse(MapReduceQueryRequest.submit(id), true);
        
        // return the id
        return id;
    }
    
    protected String submitOozieWorkflow(String workflow, MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser) throws Exception {
        // validate the job
        OozieJob oozieJob = (OozieJob) validateJob(workflow, parameters, currentUser);
        
        String id = oozieJob.createId(currentUser);
        
        // create the audit parameters from the query definition
        MultiValueMap<String,String> auditParameters = new LinkedMultiValueMap<>(parameters);
        
        // audit the job
        // @formatter:off
        queryManagementService.audit(id,
                oozieJob.getAuditType(),
                workflow,
                oozieJob.getQuery(auditParameters),
                oozieJob.getSelectors(auditParameters),
                auditParameters,
                currentUser);
        // @formatter:on
        
        // store the job in the cache
        mapReduceQueryCache.createQuery(id, workflow, parameters, currentUser);
        
        // send the job off to the mapreduce service
        sendRequestAwaitResponse(MapReduceQueryRequest.oozieSubmit(id), true);
        
        // return the id
        return id;
    }
    
    protected MultiValueMap<String,String> parseParameters(MultiValueMap<String,String> parameters) {
        List<String> encodedParams = parameters.remove(PARAMETERS);
        if (encodedParams != null) {
            for (String encodedParam : encodedParams) {
                parameters.addAll(parseParameters(encodedParam));
            }
        }
        return parameters;
    }
    
    protected MultiValueMap<String,String> parseParameters(String params) {
        MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        if (null != params) {
            String[] entries = params.split(PARAMETER_SEPARATOR);
            for (String entry : entries) {
                String[] keyValue = entry.split(PARAMETER_NAME_VALUE_SEPARATOR);
                if (keyValue.length == 2) {
                    parameters.add(keyValue[0], keyValue[1]);
                }
            }
        }
        return parameters;
    }
    
    protected MapReduceJob validateJob(String jobName, MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser)
                    throws BadRequestQueryException, UnauthorizedQueryException {
        
        // get the map reduce job
        MapReduceJob mapReduceJob = mapReduceJobs.get(jobName).get();
        if (mapReduceJob == null) {
            throw new BadRequestQueryException(DatawaveErrorCode.JOB_CONFIGURATION_ERROR, "No job configuration with name " + jobName);
        }
        
        // validate the user's roles
        validateRoles(currentUser.getPrimaryUser().getRoles(), mapReduceJob.getMapReduceJobProperties().getRequiredRoles());
        
        // validate the user's auths
        validateAuths(parameters.getFirst(QueryParameters.QUERY_AUTHORIZATIONS), mapReduceJob.getMapReduceJobProperties().getRequiredAuths());
        
        // validate the parameters
        mapReduceJob.validateParameters(parameters);
        
        return mapReduceJob;
    }
    
    protected void validateRoles(Collection<String> userRoles, Collection<String> requiredRoles) throws UnauthorizedQueryException {
        if (requiredRoles != null && !requiredRoles.isEmpty()) {
            if (!userRoles.containsAll(requiredRoles)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED,
                                MessageFormat.format("Requires the following roles: {0}", requiredRoles));
            }
        }
    }
    
    protected void validateAuths(String requestedAuths, Collection<String> requiredAuths) throws UnauthorizedQueryException {
        if (requiredAuths != null && !requiredAuths.isEmpty()) {
            Set<String> userAuths = new HashSet<>(AuthorizationsUtil.splitAuths(requestedAuths));
            if (!userAuths.containsAll(requiredAuths)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED,
                                MessageFormat.format("Requires the following auths: {0}", requiredAuths));
            }
        }
    }
    
    private void sendRequestAwaitResponse(MapReduceQueryRequest request, boolean isAwaitResponse) throws QueryException {
        if (isAwaitResponse) {
            // before publishing the message, create a latch based on the query ID
            queryLatchMap.put(request.getId(), new CountDownLatch(1));
        }
        
        // publish an event to the map reduce query service
        publishMapReduceQueryEvent(request);
        
        if (isAwaitResponse) {
            long startTimeMillis = System.currentTimeMillis();
            
            log.info("Waiting on map reduce query {} response from the map reduce query service.", request.getMethod().name());
            
            try {
                boolean isFinished = false;
                while (!isFinished && System.currentTimeMillis() < (startTimeMillis + queryProperties.getExpiration().getCallTimeoutMillis())) {
                    try {
                        // wait for the executor response
                        if (queryLatchMap.get(request.getId()).await(queryProperties.getExpiration().getCallTimeoutInterval(),
                                        queryProperties.getExpiration().getCallTimeoutIntervalUnit())) {
                            log.info("Received map reduce query {} response from the map reduce query service.", request.getMethod().name());
                            isFinished = true;
                        }
                        
                        // did the request fail?
                        MapReduceQueryStatus mapReduceQueryStatus = mapReduceQueryCache.getQueryStatus(request.getId());
                        if (mapReduceQueryStatus.getState() == FAILED) {
                            log.error("Map reduce query {} failed for id {}: {}", request.getMethod().name(), request.getId(),
                                            mapReduceQueryStatus.getFailureMessage());
                            throw new QueryException(mapReduceQueryStatus.getErrorCode(), "Map reduce query " + request.getMethod().name() + " failed for id "
                                            + request.getId() + ": " + mapReduceQueryStatus.getFailureMessage());
                        }
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while waiting for map reduce query {} latch for id {}", request.getMethod().name(), request.getId());
                    }
                }
            } finally {
                queryLatchMap.remove(request.getId());
            }
        }
    }
    
    private void publishMapReduceQueryEvent(MapReduceQueryRequest mapReduceQueryRequest) {
        // @formatter:off
        eventPublisher.publishEvent(
                new RemoteMapReduceQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        mapReduceQueryProperties.getMapReduceQueryServiceName(),
                        mapReduceQueryRequest));
        // @formatter:on
    }
    
    @Override
    public void handleRemoteRequest(MapReduceQueryRequest queryRequest, String originService, String destinationService) {
        try {
            if (queryRequest.getMethod() == SUBMIT || queryRequest.getMethod() == OOZIE_SUBMIT) {
                log.trace("Received remote {} request from {} for {}.", queryRequest.getMethod().name(), originService, destinationService);
                if (queryLatchMap.containsKey(queryRequest.getId())) {
                    queryLatchMap.get(queryRequest.getId()).countDown();
                } else {
                    log.warn("Unable to decrement {} latch for query {}", queryRequest.getMethod().name(), queryRequest.getId());
                }
            } else {
                log.debug("No handling specified for remote map reduce query request method: {} from {} for {}", queryRequest.getMethod(), originService,
                                destinationService);
            }
        } catch (Exception e) {
            log.error("Unknown error handling remote request: {} from {} for {}", queryRequest, originService, destinationService);
        }
    }
    
    public GenericResponse<Boolean> cancel(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: cancel from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        return cancel(id, currentUser, false);
    }
    
    public GenericResponse<Boolean> adminCancel(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: adminCancel from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        return cancel(id, currentUser, true);
    }
    
    private GenericResponse<Boolean> cancel(String id, DatawaveUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser, adminOverride);
            
            // if the map reduce job is submitted or running
            if (mapReduceQueryStatus.isRunning()) {
                cancel(id, mapReduceQueryStatus.getJobId(), mapReduceQueryStatus.getResultsDirectory());
                
                GenericResponse<Boolean> response = new GenericResponse<>();
                response.setResult(true);
                
                return response;
            } else {
                throw new BadRequestQueryException("Cannot call cancel on a query that is not running", HttpStatus.SC_BAD_REQUEST + "-1");
            }
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error canceling map reduce query {}", id, e);
            throw new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, "Unknown error canceling map reduce query " + id);
        }
    }
    
    private void cancel(String id, String jobId, String resultsDirectory) throws QueryException, IOException, InterruptedException {
        cancel(id, jobId);
        removeDirectory(resultsDirectory);
    }
    
    private void cancel(String id, String jobId) throws IOException, QueryException, InterruptedException {
        // cancel the map reduce job
        try (JobClient job = new JobClient(new JobConf(configuration))) {
            JobID mrJobId = JobID.forName(jobId);
            if (mrJobId instanceof org.apache.hadoop.mapred.JobID) {
                RunningJob runningJob = job.getJob((org.apache.hadoop.mapred.JobID) mrJobId);
                if (null != runningJob) {
                    // killing the job will trigger hadoop to update the status via the callback URL
                    runningJob.killJob();
                } else {
                    mapReduceQueryCache.updateQueryStatus(id, (mrQueryStatus) -> mrQueryStatus.setState(CANCELED),
                                    mapReduceQueryProperties.getLockWaitTimeMillis(), mapReduceQueryProperties.getLockLeaseTimeMillis());
                }
            }
        }
    }
    
    private void removeDirectory(String directory) throws IOException, QueryException {
        FileSystem filesystem = FileSystem.get(configuration);
        Path dir = new Path(directory);
        if (filesystem.exists(dir) && !filesystem.delete(dir, true)) {
            log.error("Unknown error deleting directory: {}", dir);
            throw new QueryException(DatawaveErrorCode.MAPRED_RESULTS_DELETE_ERROR, "Unknown error deleting directory: " + dir);
        }
    }
    
    public GenericResponse<String> restart(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: restart from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        try {
            // make sure the map reduce query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser);
            
            // if the map reduce job is submitted or running, cancel it
            if (mapReduceQueryStatus.isRunning()) {
                cancel(id, mapReduceQueryStatus.getJobId(), mapReduceQueryStatus.getResultsDirectory());
            }
            
            MultiValueMap<String,String> parsedParameters = mapReduceQueryStatus.getParameters();
            
            // make sure the original query is valid, and the user can act on it
            QueryStatus queryStatus = queryManagementService.validateRequest(parsedParameters.getFirst(QUERY_ID), currentUser);
            
            String newId = submitJob(mapReduceQueryStatus.getJobName(), parsedParameters, queryStatus, currentUser);
            GenericResponse<String> response = new GenericResponse<>();
            response.setResult(newId);
            return response;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error listing map reduce info", e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error listing map reduce info.");
        }
    }
    
    public MapReduceInfoResponseList list(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: list from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        try {
            // make sure the query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser);
            
            MapReduceInfoResponseList respList = new MapReduceInfoResponseList();
            respList.setResults(Collections.singletonList(createMapReduceInfoResponse(mapReduceQueryStatus)));
            
            return respList;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error listing map reduce info", e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error listing map reduce info.");
        }
    }
    
    protected MapReduceInfoResponse createMapReduceInfoResponse(MapReduceQueryStatus mapReduceQueryStatus) throws QueryException {
        MapReduceInfoResponse mapReduceInfoResponse = mapReduceQueryStatus.toMapReduceInfoResponse();
        
        List<LocatedFileStatus> files = listFiles(mapReduceQueryStatus.getResultsDirectory());
        List<ResultFile> resultFiles = new ArrayList<>();
        for (LocatedFileStatus file : files) {
            ResultFile resultFile = new ResultFile();
            resultFile.setFileName(file.getPath().toString());
            resultFile.setLength(file.getLen());
            resultFiles.add(resultFile);
        }
        mapReduceInfoResponse.setResultFiles(resultFiles);
        return mapReduceInfoResponse;
    }
    
    public MapReduceInfoResponseList list(DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: list for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()));
        
        try {
            Set<String> ids = mapReduceQueryCache.lookupQueryIdsByUsername(currentUser.getUsername());
            
            List<MapReduceInfoResponse> mapReduceInfoResponses = new ArrayList<>();
            for (String id : ids) {
                MapReduceQueryStatus status = mapReduceQueryCache.getQueryStatus(id);
                mapReduceInfoResponses.add(createMapReduceInfoResponse(status));
            }
            
            MapReduceInfoResponseList respList = new MapReduceInfoResponseList();
            respList.setResults(mapReduceInfoResponses);
            
            return respList;
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error listing map reduce info", e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error listing map reduce info.");
        }
    }
    
    public MapReduceQueryStatus validateRequest(String id, DatawaveUserDetails currentUser) throws NotFoundQueryException, UnauthorizedQueryException {
        return validateRequest(id, currentUser, false);
    }
    
    public MapReduceQueryStatus validateRequest(String id, DatawaveUserDetails currentUser, boolean adminOverride)
                    throws NotFoundQueryException, UnauthorizedQueryException {
        // does the map reduce job exist?
        MapReduceQueryStatus mapReduceQueryStatus = mapReduceQueryCache.getQueryStatus(id);
        if (mapReduceQueryStatus == null) {
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", id));
        }
        
        // admin requests can operate on any job, regardless of ownership
        if (!adminOverride) {
            // does the current user own this job?
            String userId = ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getDn().subjectDN());
            Query query = mapReduceQueryStatus.getQuery();
            if (!query.getOwner().equals(userId)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userId, query.getOwner()));
            }
        }
        
        return mapReduceQueryStatus;
    }
    
    public Map.Entry<FileStatus,FSDataInputStream> getFile(String id, String fileName, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: getFile from {} for {}, {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id, fileName);
        
        try {
            // make sure the query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser);
            
            String resultsDirectory = mapReduceQueryStatus.getResultsDirectory();
            
            Path resultsPath = new Path(new URI(resultsDirectory));
            Path resultFile = new Path(resultsPath, fileName);
            
            FileSystem filesystem = FileSystem.get(configuration);
            FileStatus fileStatus = filesystem.getFileStatus(resultFile);
            
            if (!fileStatus.isFile()) {
                throw new BadRequestQueryException("Requested path is not a file: " + fileName, HttpStatus.SC_BAD_REQUEST + "-1");
            }
            
            // update the file status to reflect a relative file path
            fileStatus.setPath(getRelativeFilePath(filesystem.getFileStatus(resultsPath), fileStatus));
            
            return new AbstractMap.SimpleEntry<>(fileStatus, getFileInputStream(resultFile));
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error retrieving result file", e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error retrieving result file.");
        }
    }
    
    private FSDataInputStream getFileInputStream(FileSystem filesystem, Path filePath) throws QueryException {
        try {
            return filesystem.open(filePath);
        } catch (IOException e) {
            throw new NotFoundQueryException("Unable to open result file", e, HttpStatus.SC_INTERNAL_SERVER_ERROR + "-1");
        }
    }
    
    private FSDataInputStream getFileInputStream(Path filePath) throws QueryException, IOException {
        return getFileInputStream(FileSystem.get(configuration), filePath);
    }
    
    public Map<FileStatus,FSDataInputStream> getAllFiles(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: getAllFiles from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        Map<FileStatus,FSDataInputStream> resultFiles = new HashMap<>();
        try {
            // make sure the query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser);
            
            FileSystem fs = FileSystem.get(configuration);
            
            FileStatus basePathStatus = fs.getFileStatus(new Path(mapReduceQueryStatus.getResultsDirectory()));
            
            if (mapReduceQueryStatus.getResultsDirectory() != null && !mapReduceQueryStatus.getResultsDirectory().isEmpty()) {
                for (LocatedFileStatus fileStatus : listFiles(mapReduceQueryStatus.getResultsDirectory())) {
                    if (fileStatus.isFile()) {
                        FSDataInputStream inputStream = getFileInputStream(fs, fileStatus.getPath());
                        
                        // update the file status to reflect a relative file path
                        fileStatus.setPath(getRelativeFilePath(basePathStatus, fileStatus));
                        
                        resultFiles.put(fileStatus, inputStream);
                    }
                }
            }
            return resultFiles;
        } catch (QueryException e) {
            for (Map.Entry<FileStatus,FSDataInputStream> resultFile : resultFiles.entrySet()) {
                try {
                    resultFile.getValue().close();
                } catch (Exception ex) {
                    log.error("Unknown error closing input stream", e);
                }
            }
            
            throw e;
        } catch (Exception e) {
            for (Map.Entry<FileStatus,FSDataInputStream> resultFile : resultFiles.entrySet()) {
                try {
                    resultFile.getValue().close();
                } catch (Exception ex) {
                    log.error("Unknown error closing input stream", e);
                }
            }
            
            log.error("Unknown error retrieving result file", e);
            throw new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, "Unknown error retrieving result file.");
        }
    }
    
    private Path getRelativeFilePath(FileStatus basePath, FileStatus filePath) {
        int basePathLength = basePath.getPath().toUri().getPath().length();
        return new Path(filePath.getPath().toUri().getPath().substring(basePathLength + 1));
    }
    
    public VoidResponse remove(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: remove from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        return remove(id, currentUser, false);
    }
    
    public VoidResponse adminRemove(String id, DatawaveUserDetails currentUser) throws QueryException {
        log.info("Request: adminRemove from {} for {}", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), id);
        
        return remove(id, currentUser, true);
    }
    
    private VoidResponse remove(String id, DatawaveUserDetails currentUser, boolean adminOverride) throws QueryException {
        try {
            // make sure the query is valid, and the user can act on it
            MapReduceQueryStatus mapReduceQueryStatus = validateRequest(id, currentUser, adminOverride);
            
            // if the map reduce job is submitted or running, cancel it
            if (mapReduceQueryStatus.isRunning()) {
                cancel(id, mapReduceQueryStatus.getJobId());
            }
            
            // remove the working directory
            removeDirectory(mapReduceQueryStatus.getWorkingDirectory());
            
            // remove the cache entry
            mapReduceQueryCache.removeQuery(mapReduceQueryStatus.getId());
            
            return new VoidResponse();
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unknown error removing map reduce query {}", id, e);
            throw new QueryException(DatawaveErrorCode.QUERY_REMOVAL_ERROR, e, "Unknown error removing map reduce query " + id);
        }
    }
    
    private List<LocatedFileStatus> listFiles(String resultsDirectory) throws QueryException {
        try {
            List<LocatedFileStatus> files = new ArrayList<>();
            
            if (resultsDirectory != null && !resultsDirectory.isEmpty()) {
                FileSystem fs = FileSystem.get(configuration);
                Path resultsPath = new Path(new URI(resultsDirectory));
                if (fs.exists(resultsPath)) {
                    RemoteIterator<LocatedFileStatus> fileStatusIter = fs.listFiles(resultsPath, true);
                    while (fileStatusIter.hasNext()) {
                        LocatedFileStatus fileStatus = fileStatusIter.next();
                        if (fileStatus.isFile()) {
                            files.add(fileStatus);
                        }
                    }
                }
            }
            
            return files;
        } catch (Exception e) {
            log.error("Unknown error listing files", e);
            throw new QueryException(DatawaveErrorCode.FILE_LIST_ERROR, e, "Unknown error listing files.");
        }
    }
    
    public VoidResponse updateState(String jobId, String jobStatus) throws QueryException {
        log.info("Request: updateState for {} to {}", jobId, jobStatus);
        
        VoidResponse response = new VoidResponse();
        try {
            String id = mapReduceQueryCache.lookupQueryIdByJobId(jobId);
            if (id != null) {
                mapReduceQueryCache.updateQueryStatus(id, (mapReduceQueryStatus) -> {
                    mapReduceQueryStatus.setState(JobStatus.State.valueOf(jobStatus));
                }, mapReduceQueryProperties.getLockWaitTimeMillis(), mapReduceQueryProperties.getLockLeaseTimeMillis());
            }
            return response;
        } catch (Exception e) {
            throw new QueryException(DatawaveErrorCode.MAPRED_UPDATE_STATUS_ERROR, e);
        }
    }
}
