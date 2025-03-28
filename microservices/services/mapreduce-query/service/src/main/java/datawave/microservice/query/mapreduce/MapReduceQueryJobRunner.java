package datawave.microservice.query.mapreduce;

import static datawave.microservice.query.mapreduce.jobs.OozieJob.OOZIE_CLIENT;
import static datawave.microservice.query.mapreduce.jobs.OozieJob.OUT_DIR;
import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.OOZIE_SUBMIT;
import static datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest.Method.SUBMIT;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteMapReduceQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.mapreduce.jobs.MapReduceJob;
import datawave.microservice.query.mapreduce.jobs.OozieJob;
import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest;
import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequestHandler;
import datawave.microservice.query.mapreduce.status.MapReduceQueryCache;
import datawave.microservice.query.mapreduce.status.MapReduceQueryStatus;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

@Service
public class MapReduceQueryJobRunner implements MapReduceQueryRequestHandler {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final QueryProperties queryProperties;
    
    private final MapReduceQueryProperties mapReduceQueryProperties;
    
    private final ApplicationEventPublisher eventPublisher;
    
    private final BusProperties busProperties;
    
    private final MapReduceQueryCache mapReduceQueryCache;
    
    private final QueryLogicFactory queryLogicFactory;
    
    private final AccumuloConnectionFactory accumuloConnectionFactory;
    
    private final Map<String,Supplier<MapReduceJob>> mapReduceJobs;
    
    private final Configuration configuration;
    
    public MapReduceQueryJobRunner(QueryProperties queryProperties, MapReduceQueryProperties mapReduceQueryProperties, ApplicationEventPublisher eventPublisher,
                    BusProperties busProperties, MapReduceQueryCache mapReduceQueryCache, QueryLogicFactory queryLogicFactory,
                    AccumuloConnectionFactory accumuloConnectionFactory, Map<String,Supplier<MapReduceJob>> mapReduceJobs) {
        this.queryProperties = queryProperties;
        this.mapReduceQueryProperties = mapReduceQueryProperties;
        this.eventPublisher = eventPublisher;
        this.busProperties = busProperties;
        this.mapReduceQueryCache = mapReduceQueryCache;
        this.queryLogicFactory = queryLogicFactory;
        this.accumuloConnectionFactory = accumuloConnectionFactory;
        this.mapReduceJobs = mapReduceJobs;
        
        this.configuration = new Configuration();
        if (mapReduceQueryProperties.getFsConfigResources() != null) {
            for (String resource : mapReduceQueryProperties.getFsConfigResources()) {
                this.configuration.addResource(new Path(resource));
            }
        }
    }
    
    @Override
    public void handleRemoteRequest(MapReduceQueryRequest mapReduceQueryRequest, String originService, String destinationService) {
        log.info("Received map reduce query {} request for id {}", mapReduceQueryRequest.getMethod().name(), mapReduceQueryRequest.getId());
        
        try {
            if (mapReduceQueryRequest.getMethod() == SUBMIT) {
                mapReduceQueryCache.updateQueryStatus(mapReduceQueryRequest.getId(), this::submit, queryProperties.getLockWaitTimeMillis(),
                                queryProperties.getLockLeaseTimeMillis());
                
                notifyOriginOfCreation(originService, MapReduceQueryRequest.submit(mapReduceQueryRequest.getId()));
            } else if (mapReduceQueryRequest.getMethod() == OOZIE_SUBMIT) {
                mapReduceQueryCache.updateQueryStatus(mapReduceQueryRequest.getId(), this::oozieSubmit, queryProperties.getLockWaitTimeMillis(),
                                queryProperties.getLockLeaseTimeMillis());
                
                notifyOriginOfCreation(originService, MapReduceQueryRequest.oozieSubmit(mapReduceQueryRequest.getId()));
            } else {
                log.warn("Unknown method for map reduce query request: {}", mapReduceQueryRequest);
            }
        } catch (Exception e) {
            log.error("Unknown error handling remote map reduce query request", e);
        }
    }
    
    private void submit(MapReduceQueryStatus mapReduceQueryStatus) {
        try {
            // submit the job
            // @formatter:off
            StringBuilder name = new StringBuilder()
                    .append(mapReduceQueryStatus.getJobName())
                    .append("_sid_")
                    .append(ProxiedEntityUtils.getShortName(mapReduceQueryStatus.getCurrentUser().getPrimaryUser().getName()))
                    .append("_id_")
                    .append(mapReduceQueryStatus.getId());
            // @formatter:on
            
            MapReduceJob mapReduceJob = mapReduceJobs.get(mapReduceQueryStatus.getJobName()).get();
            
            Job job;
            try {
                job = Job.getInstance(configuration, name.toString());
                mapReduceJob.initializeConfiguration(queryLogicFactory, accumuloConnectionFactory, mapReduceQueryStatus, job);
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.LOGIC_CONFIGURATION_ERROR, e);
                log.error(qe.getMessage(), e);
                throw qe;
            }
            
            // Enforce that certain InputFormat classes are being used here.
            if (mapReduceQueryProperties.isRestrictInputFormats()) {
                // Make sure that the job input format is in the list
                Class<? extends InputFormat<?,?>> ifClass;
                try {
                    ifClass = job.getInputFormatClass();
                } catch (ClassNotFoundException e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.INPUT_FORMAT_CLASS_ERROR, e);
                    log.error(qe.getMessage(), e);
                    throw qe;
                }
                if (!mapReduceQueryProperties.getValidInputFormats().contains(ifClass)) {
                    IllegalArgumentException e = new IllegalArgumentException(
                                    "Invalid input format class specified. Must use one of " + mapReduceQueryProperties.getValidInputFormats());
                    QueryException qe = new QueryException(DatawaveErrorCode.INVALID_FORMAT, e);
                    log.error(qe.getMessage(), e);
                    throw qe;
                }
            }
            
            try {
                job.submit();
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.MAPREDUCE_JOB_START_ERROR, e);
                log.error(qe.getMessage(), qe);
                throw qe;
            }
            
            JobID mapReduceJobId = job.getJobID();
            log.info("JOB ID: " + mapReduceJobId);
            
            // This is used to lookup the query id when the mapreduce callback comes in
            mapReduceQueryCache.putQueryIdByJobIdLookup(mapReduceJobId.toString(), mapReduceQueryStatus.getId());
            
            mapReduceQueryStatus.setJobId(mapReduceJobId.toString());
            mapReduceQueryStatus.setJobTracker(mapReduceJob.getMapReduceJobProperties().getJobTracker());
            mapReduceQueryStatus.setHdfsUri(mapReduceJob.getMapReduceJobProperties().getHdfsUri());
            mapReduceQueryStatus.setState(MapReduceQueryStatus.MapReduceQueryState.SUBMITTED);
        } catch (Exception e) {
            // @formatter:off
            mapReduceQueryStatus.setErrorCode((e instanceof QueryException) ?
                    DatawaveErrorCode.findCode(((QueryException) e).getErrorCode()) :
                    DatawaveErrorCode.UNKNOWN_SERVER_ERROR);
            // @formatter:on
            
            mapReduceQueryStatus.setFailureMessage(e.getMessage());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            e.printStackTrace(writer);
            writer.close();
            mapReduceQueryStatus.setState(MapReduceQueryStatus.MapReduceQueryState.FAILED);
            mapReduceQueryStatus.setStackTrace(new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
        }
    }
    
    private void oozieSubmit(MapReduceQueryStatus mapReduceQueryStatus) {
        OozieClient oozieClient = null;
        String jobId = null;
        try {
            OozieJob oozieJob = (OozieJob) mapReduceJobs.get(mapReduceQueryStatus.getJobName()).get();
            
            oozieClient = new OozieClient(oozieJob.getMapReduceJobProperties().getJobConfigurationProperties().get(OOZIE_CLIENT));
            Properties oozieProperties = oozieClient.createConfiguration();
            oozieJob.initializeOozieConfiguration(mapReduceQueryStatus.getId(), oozieProperties, mapReduceQueryStatus.getParameters());
            oozieJob.validateWorkflowParameter(oozieProperties);
            
            try {
                jobId = oozieClient.run(oozieProperties);
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.OOZIE_JOB_START_ERROR, e);
                log.error(qe.getMessage(), e);
                throw qe;
            }
            
            log.info("JOB ID: " + jobId);
            
            // This is used to lookup the query id when the mapreduce callback comes in
            mapReduceQueryCache.putQueryIdByJobIdLookup(jobId, mapReduceQueryStatus.getId());
            
            // the oozie workflow definitions will add the workflow id to outDir to get the full output path
            Path baseDir = new Path(mapReduceQueryProperties.getMapReduceBaseDirectory());
            Path jobDir = new Path(baseDir, mapReduceQueryStatus.getId());
            
            mapReduceQueryStatus.setJobId(jobId);
            mapReduceQueryStatus.setWorkingDirectory(jobDir.toString());
            mapReduceQueryStatus.setResultsDirectory(oozieProperties.getProperty(OUT_DIR) + "/" + mapReduceQueryStatus.getId());
            mapReduceQueryStatus.setJobTracker(oozieJob.getMapReduceJobProperties().getJobTracker());
            mapReduceQueryStatus.setHdfsUri(oozieJob.getMapReduceJobProperties().getHdfsUri());
            mapReduceQueryStatus.setState(MapReduceQueryStatus.MapReduceQueryState.SUBMITTED);
        } catch (QueryException e) {
            mapReduceQueryStatus.setErrorCode(DatawaveErrorCode.findCode(e.getErrorCode()));
            mapReduceQueryStatus.setFailureMessage(e.getMessage());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            e.printStackTrace(writer);
            writer.close();
            mapReduceQueryStatus.setState(MapReduceQueryStatus.MapReduceQueryState.FAILED);
            mapReduceQueryStatus.setStackTrace(new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
            
            try {
                oozieClient.kill(jobId);
            } catch (Exception e2) {
                log.error("Unable to kill oozie workflow: {}", jobId);
            }
        } catch (Exception e) {
            mapReduceQueryStatus.setErrorCode(DatawaveErrorCode.UNKNOWN_SERVER_ERROR);
            mapReduceQueryStatus.setFailureMessage(e.getMessage());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            e.printStackTrace(writer);
            writer.close();
            mapReduceQueryStatus.setState(MapReduceQueryStatus.MapReduceQueryState.FAILED);
            mapReduceQueryStatus.setStackTrace(new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
            
            try {
                oozieClient.kill(jobId);
            } catch (Exception e2) {
                log.error("Unable to kill oozie workflow: {}", jobId);
            }
        }
    }
    
    private void notifyOriginOfCreation(String originService, MapReduceQueryRequest request) {
        log.debug("Publishing a submit request to the originating service: " + originService);
        // @formatter:off
        eventPublisher.publishEvent(
                new RemoteMapReduceQueryRequestEvent(
                        this,
                        busProperties.getId(),
                        originService,
                        request));
        // @formatter:on
    }
}
