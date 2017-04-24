package nsa.datawave.webservice.mr;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;

import nsa.datawave.annotation.Required;
import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.marking.SecurityMarking;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.security.authorization.PrincipalFactory;
import nsa.datawave.security.system.ServerPrincipal;
import nsa.datawave.security.util.AuthorizationsUtil;
import nsa.datawave.webservice.common.audit.AuditBean;
import nsa.datawave.webservice.common.audit.AuditParameters;
import nsa.datawave.webservice.common.audit.Auditor;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.common.connection.config.ConnectionPoolsConfiguration;
import nsa.datawave.webservice.common.exception.BadRequestException;
import nsa.datawave.webservice.common.exception.DatawaveWebApplicationException;
import nsa.datawave.webservice.common.exception.NotFoundException;
import nsa.datawave.webservice.common.exception.UnauthorizedException;
import nsa.datawave.webservice.mr.configuration.MapReduceConfiguration;
import nsa.datawave.webservice.mr.configuration.MapReduceJobConfiguration;
import nsa.datawave.webservice.mr.configuration.NeedAccumuloConnectionFactory;
import nsa.datawave.webservice.mr.configuration.NeedAccumuloDetails;
import nsa.datawave.webservice.mr.configuration.NeedCallerDetails;
import nsa.datawave.webservice.mr.configuration.NeedPrincipalFactory;
import nsa.datawave.webservice.mr.configuration.NeedQueryCache;
import nsa.datawave.webservice.mr.configuration.NeedQueryLogicFactory;
import nsa.datawave.webservice.mr.configuration.NeedQueryPersister;
import nsa.datawave.webservice.mr.configuration.OozieJobConfiguration;
import nsa.datawave.webservice.mr.configuration.OozieJobConstants;
import nsa.datawave.webservice.mr.state.MapReduceStatePersisterBean;
import nsa.datawave.webservice.mr.state.MapReduceStatePersisterBean.MapReduceState;
import nsa.datawave.webservice.query.cache.QueryCache;
import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.NotFoundQueryException;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.exception.UnauthorizedQueryException;
import nsa.datawave.webservice.query.factory.Persister;
import nsa.datawave.webservice.query.logic.QueryLogicFactory;
import nsa.datawave.webservice.result.BaseResponse;
import nsa.datawave.webservice.result.GenericResponse;
import nsa.datawave.webservice.result.VoidResponse;
import nsa.datawave.webservice.results.mr.JobExecution;
import nsa.datawave.webservice.results.mr.MapReduceInfoResponse;
import nsa.datawave.webservice.results.mr.MapReduceInfoResponseList;
import nsa.datawave.webservice.results.mr.MapReduceJobDescription;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

@javax.ws.rs.Path("/MapReduce")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class MapReduceBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Resource
    private EJBContext ctx;
    
    @Inject
    private Persister queryPersister;
    
    @Inject
    private QueryCache cache;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private QueryLogicFactory queryLogicFactory;
    
    @Inject
    private MapReduceStatePersisterBean mapReduceState;
    
    // reference "datawave/common/ConnectionPools.xml" and "datawave/mapreduce/MapReduceJobs.xml"
    @Inject
    @SpringBean(refreshable = true)
    private MapReduceConfiguration mapReduceConfiguration;
    
    @Inject
    private ConnectionPoolsConfiguration connectionPoolsConfiguration;
    
    @Inject
    @ServerPrincipal
    private DatawavePrincipal serverPrincipal;
    
    @Inject
    private PrincipalFactory principalFactory;
    
    @Inject
    private SecurityMarking marking;
    
    @Inject
    private AuditParameters auditParameters;
    
    @Inject
    private AuditBean auditor;
    
    private static final String PARAMETER_SEPARATOR = ";";
    private static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    private static final String JOB_ID = "MapReduce.id";
    private static final int BUFFER_SIZE = 16384;
    
    /**
     * Returns a list of the MapReduce job names and their configurations
     *
     * @param jobType
     *            limit returned jobs configs to the specified type
     * @return nsa.datawave.webservice.results.mr.MapReduceJobDescription
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/listConfigurations")
    @GZIP
    public List<MapReduceJobDescription> listConfiguredMapReduceJobs(@DefaultValue("none") @QueryParam("jobType") String jobType) {
        List<MapReduceJobDescription> jobs = new ArrayList<>();
        if (jobType.equals("none")) {
            jobType = null;
        }
        for (Entry<String,MapReduceJobConfiguration> entry : this.mapReduceConfiguration.getJobConfiguration().entrySet()) {
            if (jobType != null && !entry.getValue().getJobType().equals(jobType)) {
                continue;
            }
            jobs.add(entry.getValue().getConfigurationDescription(entry.getKey()));
        }
        return jobs;
    }
    
    /**
     * Execute a Oozie workflow with the given workFlow name and runtime parameters
     * 
     * @param workFlow
     * @param parameters
     * @return
     */
    
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/ooziesubmit")
    @GZIP
    public GenericResponse<String> ooziesubmit(MultivaluedMap<String,String> queryParameters) {
        GenericResponse<String> response = new GenericResponse<>();
        
        String workFlow = queryParameters.getFirst(OozieJobConstants.WORKFLOW_PARAM);
        if (StringUtils.isBlank(workFlow)) {
            throw new BadRequestException(new IllegalArgumentException(OozieJobConstants.WORKFLOW_PARAM + " parameter missing"), response);
        }
        String parameters = queryParameters.getFirst(OozieJobConstants.PARAMETERS);
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        
        Set<Collection<String>> userRoles = new HashSet<>();
        String sid = null;
        String userDn = p.getName();
        DatawavePrincipal datawavePrincipal = null;
        if (p instanceof DatawavePrincipal) {
            datawavePrincipal = (DatawavePrincipal) p;
            userRoles.addAll(datawavePrincipal.getUserRoles());
            sid = datawavePrincipal.getShortName();
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_PRINCIPAL_ERROR, MessageFormat.format("Class: {0}", p.getClass().getName()));
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        OozieJobConfiguration job;
        try {
            MapReduceJobConfiguration mrConfig = this.mapReduceConfiguration.getConfiguration(workFlow);
            if (mrConfig instanceof OozieJobConfiguration) {
                job = (OozieJobConfiguration) mrConfig;
            } else {
                throw new IllegalArgumentException(workFlow + " not an Oozie job configuration");
            }
        } catch (IllegalArgumentException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JOB_CONFIGURATION_ERROR, e);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        
        if (job instanceof NeedCallerDetails) {
            ((NeedCallerDetails) job).setUserSid(sid);
            ((NeedCallerDetails) job).setPrincipal(p);
        }
        
        if (job instanceof NeedPrincipalFactory) {
            ((NeedPrincipalFactory) job).setPrincipalFactory(principalFactory);
        }
        
        // Ensure that the user has the required roles and has passed the required auths
        if (null != job.getRequiredRoles() || null != job.getRequiredAuths()) {
            try {
                canRunJob(datawavePrincipal, queryParameters, job.getRequiredRoles(), job.getRequiredAuths());
            } catch (UnauthorizedQueryException qe) {
                // user does not have all of the required roles or did not pass the required auths
                response.addException(qe);
                throw new UnauthorizedException(qe, response);
            }
        }
        
        String id = sid + "_" + UUID.randomUUID().toString();
        OozieClient oozieClient = null;
        Properties oozieConf = null;
        
        try {
            oozieClient = new OozieClient(job.getJobConfigurationProperties().get(OozieJobConstants.OOZIE_CLIENT_PROP));
            oozieConf = oozieClient.createConfiguration();
            job.initializeOozieConfiguration(id, oozieConf, queryParameters);
            job.validateWorkflowParameter(oozieConf, mapReduceConfiguration);
        } catch (QueryException qe) {
            log.error(qe.getMessage(), qe);
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(new QueryException(e.getMessage(), e));
            throw new DatawaveWebApplicationException(e, response);
        } finally {
            // audit query here
            Auditor.AuditType auditType = job.getAuditType();
            log.trace("Audit type is: " + auditType.name());
            if (!auditType.equals(Auditor.AuditType.NONE)) {
                try {
                    marking.validate(queryParameters);
                    MultivaluedMap<String,String> auditQueryParameters = job.getAuditParameters(queryParameters, oozieConf,
                                    auditParameters.getRequiredAuditParameters());
                    auditQueryParameters.add(AuditParameters.USER_DN, userDn);
                    auditQueryParameters.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, marking.toColumnVisibilityString());
                    auditQueryParameters.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
                    
                    auditParameters.clear();
                    auditParameters.validate(auditQueryParameters);
                    auditParameters.setSelectors(job.getSelectors(queryParameters, oozieConf));
                    log.debug("sending audit message: " + auditParameters);
                    auditor.audit(auditParameters);
                } catch (IllegalArgumentException e) {
                    log.error("Error validating audit parameters", e);
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
                    response.addException(qe);
                    throw new BadRequestException(qe, response);
                } catch (Exception e) {
                    log.error("Error auditing query", e);
                    response.addMessage("Error auditing query - " + e.getMessage());
                    throw new BadRequestException(e, response);
                }
            }
        }
        // Submit the Oozie workflow.
        try {
            String jobID = null;
            try {
                jobID = oozieClient.run(oozieConf);
            } catch (Exception e) {
                throw new QueryException(DatawaveErrorCode.OOZIE_JOB_START_ERROR, e);
            }
            // the oozie workflow definitions will add the workflow id to outDir to get the full output path
            
            try {
                String jobResultstDir = oozieConf.getProperty(OozieJobConstants.OUT_DIR_PROP) + "/" + id;
                response.setResult(id);
                Path baseDir = new Path(this.mapReduceConfiguration.getMapReduceBaseDirectory());
                // Create a directory path for this job
                Path jobDir = new Path(baseDir, id);
                mapReduceState.create(id, job.getHdfsUri(), job.getJobTracker(), jobDir.toString(), jobID, jobResultstDir, parameters, workFlow);
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.MAPREDUCE_STATE_PERSISTENCE_ERROR, e);
                response.addException(qe.getBottomQueryException());
                try {
                    oozieClient.kill(jobID);
                    // if we successfully kill the job, throw the original exception
                    throw qe;
                } catch (Exception e2) {
                    // throw the exception from killing the job
                    throw new QueryException(DatawaveErrorCode.MAPREDUCE_JOB_KILL_ERROR, e2);
                }
            }
        } catch (QueryException qe) {
            log.error(qe.getMessage(), qe);
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            QueryException qe = new QueryException(DatawaveErrorCode.UNKNOWN_SERVER_ERROR, e.getMessage());
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        return response;
    }
    
    /**
     * Execute a MapReduce job with the given name and runtime parameters
     *
     * @param jobName
     *            Name of the map reduce job configuration
     * @param parameters
     *            A semi-colon separated list name:value pairs. These are the required and optional parameters listed in the MapReduceConfiguration objects
     *            returned in the call to list()
     * @return nsa.datawave.webservice.result.GenericResponse<String> job id
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 204 if no data was found
     * @HTTP 400 if jobName is invalid
     * @HTTP 401 if user does not have correct roles
     * @HTTP 500 error starting the job
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/submit")
    @GZIP
    public GenericResponse<String> submit(@FormParam("jobName") String jobName, @FormParam("parameters") String parameters) {
        GenericResponse<String> response = new GenericResponse<>();
        
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid;
        Set<Collection<String>> cbAuths = new HashSet<>();
        Set<Collection<String>> userRoles = new HashSet<>();
        DatawavePrincipal datawavePrincipal = null;
        
        if (p instanceof DatawavePrincipal) {
            datawavePrincipal = (DatawavePrincipal) p;
            sid = datawavePrincipal.getShortName();
            userRoles.addAll(datawavePrincipal.getUserRoles());
            cbAuths.addAll(datawavePrincipal.getAuthorizations());
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.UNEXPECTED_PRINCIPAL_ERROR, MessageFormat.format("Class: {0}", p.getClass().getName()));
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        // Get the MapReduceJobConfiguration from the configuration
        MapReduceJobConfiguration job;
        try {
            job = this.mapReduceConfiguration.getConfiguration(jobName);
        } catch (IllegalArgumentException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JOB_CONFIGURATION_ERROR, e);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        
        // Ensure that the user has the required roles and has passed the required auths
        if (null != job.getRequiredRoles() || null != job.getRequiredAuths()) {
            try {
                canRunJob(datawavePrincipal, new MultivaluedMapImpl<String,String>(), job.getRequiredRoles(), job.getRequiredAuths());
            } catch (UnauthorizedQueryException qe) {
                // user does not have all of the required roles or did not pass the required auths
                response.addException(qe);
                throw new UnauthorizedException(qe, response);
            }
        }
        
        // Parse the parameters
        Map<String,String> runtimeParameters = new HashMap<>();
        if (null != parameters) {
            String[] param = parameters.split(PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    runtimeParameters.put(parts[0], parts[1]);
                }
            }
        }
        
        // Check to see if the job configuration class implements specific interfaces.
        if (job instanceof NeedCallerDetails) {
            ((NeedCallerDetails) job).setUserSid(sid);
            ((NeedCallerDetails) job).setPrincipal(p);
        }
        if (job instanceof NeedAccumuloConnectionFactory) {
            ((NeedAccumuloConnectionFactory) job).setAccumuloConnectionFactory(this.connectionFactory);
        }
        if (job instanceof NeedAccumuloDetails) {
            ((NeedAccumuloDetails) job).setUsername(this.connectionPoolsConfiguration.getPools().get(this.connectionPoolsConfiguration.getDefaultPool())
                            .getUsername());
            ((NeedAccumuloDetails) job).setPassword(this.connectionPoolsConfiguration.getPools().get(this.connectionPoolsConfiguration.getDefaultPool())
                            .getPassword());
            ((NeedAccumuloDetails) job).setInstanceName(this.connectionPoolsConfiguration.getPools().get(this.connectionPoolsConfiguration.getDefaultPool())
                            .getInstance());
            ((NeedAccumuloDetails) job).setZookeepers(this.connectionPoolsConfiguration.getPools().get(this.connectionPoolsConfiguration.getDefaultPool())
                            .getZookeepers());
        }
        if (job instanceof NeedQueryLogicFactory) {
            ((NeedQueryLogicFactory) job).setQueryLogicFactory(this.queryLogicFactory);
        }
        if (job instanceof NeedQueryPersister) {
            ((NeedQueryPersister) job).setPersister(this.queryPersister);
        }
        
        if (job instanceof NeedQueryCache) {
            ((NeedQueryCache) job).setQueryCache(cache);
        }
        
        if (job instanceof NeedPrincipalFactory) {
            ((NeedPrincipalFactory) job).setPrincipalFactory(principalFactory);
        }
        
        // If this job is being restarted, then the jobId will be the same. The restart method
        // puts the id into the runtime parameters
        String id = runtimeParameters.get(JOB_ID);
        if (null == id)
            id = UUID.randomUUID().toString();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        
        StringBuilder name = new StringBuilder().append(jobName).append("_sid_").append(sid).append("_id_").append(id);
        Job j;
        try {
            j = createJob(conf, name);
            job.initializeConfiguration(id, j, runtimeParameters, serverPrincipal);
        } catch (WebApplicationException waEx) {
            throw waEx;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.LOGIC_CONFIGURATION_ERROR, e);
            log.error(qe.getMessage(), e);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        // Enforce that certain InputFormat classes are being used here.
        if (this.mapReduceConfiguration.isRestrictInputFormats()) {
            // Make sure that the job input format is in the list
            Class<? extends InputFormat<?,?>> ifClass;
            try {
                ifClass = j.getInputFormatClass();
            } catch (ClassNotFoundException e1) {
                QueryException qe = new QueryException(DatawaveErrorCode.INPUT_FORMAT_CLASS_ERROR, e1);
                log.error(qe);
                response.addException(qe);
                throw new DatawaveWebApplicationException(qe, response);
            }
            if (!this.mapReduceConfiguration.getValidInputFormats().contains(ifClass)) {
                IllegalArgumentException e = new IllegalArgumentException("Invalid input format class specified. Must use one of "
                                + this.mapReduceConfiguration.getValidInputFormats().toString());
                QueryException qe = new QueryException(DatawaveErrorCode.INVALID_FORMAT, e);
                log.error(qe);
                response.addException(qe.getBottomQueryException());
                throw new DatawaveWebApplicationException(qe, response);
            }
        }
        
        try {
            j.submit();
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MAPREDUCE_JOB_START_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        JobID mapReduceJobId = j.getJobID();
        log.info("JOB ID: " + mapReduceJobId);
        
        // Create an entry in the state table
        boolean restarted = (runtimeParameters.get(JOB_ID) != null);
        try {
            if (!restarted)
                mapReduceState.create(id, job.getHdfsUri(), job.getJobTracker(), job.getJobDir(), mapReduceJobId.toString(), job.getResultsDir(), parameters,
                                jobName);
            else
                mapReduceState.addJob(id, mapReduceJobId.toString());
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.MAPREDUCE_STATE_PERSISTENCE_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            try {
                j.killJob();
            } catch (IOException ioe) {
                QueryException qe2 = new QueryException(DatawaveErrorCode.MAPREDUCE_JOB_KILL_ERROR, ioe);
                response.addException(qe2);
            }
            throw new DatawaveWebApplicationException(qe, response);
            
        }
        response.setResult(id);
        return response;
        
    }
    
    protected Job createJob(Configuration conf, StringBuilder name) throws IOException {
        return Job.getInstance(conf, name.toString());
    }
    
    /**
     * Cancels any MapReduce jobs with the specified jobId and clears out the results directory
     *
     * @param jobId
     * @return nsa.datawave.webservice.result.GenericResponse<Boolean>
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500 error killing the job
     */
    @PUT
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/{jobId}/cancel")
    @GZIP
    public GenericResponse<Boolean> cancel(@PathParam("jobId") String jobId) {
        GenericResponse<Boolean> response = new GenericResponse<>();
        
        // Find all potential running jobs
        MapReduceInfoResponseList list = mapReduceState.findById(jobId);
        List<String> jobIdsToKill = new ArrayList<>();
        // Should contain zero or one bulk result job
        if (list.getResults().size() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_MAPREDUCE_OBJECT_MATCH);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        } else if (list.getResults().size() > 1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_MAPREDUCE_OBJECT_MATCHES);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        } else {
            MapReduceInfoResponse thisJob = list.getResults().get(0);
            // Get all the executions for this job
            String prevId = null;
            String prevState = null;
            // if we have a started state, but no other state for this job, then
            // add it to the kill list
            for (JobExecution ex : thisJob.getJobExecutions()) {
                if (prevId != null) {
                    if (prevState.equals(MapReduceState.STARTED.toString()) && !ex.getMapReduceJobId().equals(prevId))
                        jobIdsToKill.add(prevId);
                    
                }
                prevId = ex.getMapReduceJobId();
                prevState = ex.getState();
            }
            // Get the last one
            if (MapReduceState.STARTED.toString().equals(prevState))
                jobIdsToKill.add(prevId);
            
            FileSystem hdfs = null;
            try {
                hdfs = getFS(thisJob.getHdfs(), response);
                Path resultsDir = new Path(thisJob.getResultsDirectory());
                hdfs.getConf().set("mapreduce.jobtracker.address", thisJob.getJobTracker());
                // Create a Job object
                JobClient job = new JobClient(new JobConf(hdfs.getConf()));
                for (String killId : jobIdsToKill) {
                    try {
                        JobID jid = JobID.forName(killId);
                        RunningJob rj = job.getJob(new org.apache.hadoop.mapred.JobID(jid.getJtIdentifier(), jid.getId()));
                        // job.getJob(jid);
                        if (null != rj)
                            rj.killJob();
                        else
                            mapReduceState.updateState(killId, MapReduceState.KILLED);
                    } catch (IOException | QueryException e) {
                        QueryException qe = new QueryException(DatawaveErrorCode.MAPREDUCE_JOB_KILL_ERROR, e, MessageFormat.format("job_id: {0}", killId));
                        log.error(qe);
                        response.addException(qe.getBottomQueryException());
                        throw new DatawaveWebApplicationException(qe, response);
                    }
                }
                // Delete the contents of the results directory
                if (hdfs.exists(resultsDir) && !hdfs.delete(resultsDir, true)) {
                    QueryException qe = new QueryException(DatawaveErrorCode.MAPRED_RESULTS_DELETE_ERROR, MessageFormat.format("directory: {0}",
                                    resultsDir.toString()));
                    log.error(qe);
                    response.addException(qe);
                    throw new DatawaveWebApplicationException(qe, response);
                }
                response.setResult(true);
                return response;
            } catch (IOException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.JOBTRACKER_CONNECTION_ERROR, e, MessageFormat.format("JobTracker: {0}",
                                thisJob.getJobTracker()));
                log.error(qe);
                response.addException(qe);
                throw new DatawaveWebApplicationException(qe, response);
            } finally {
                if (null != hdfs) {
                    try {
                        hdfs.close();
                    } catch (IOException e) {
                        log.error("Error closing HDFS client", e);
                    }
                }
            }
        }
    }
    
    /**
     * Kill any job running associated with the BulkResults id and start a new job.
     *
     * @param jobId
     * @return nsa.datawave.webservice.result.GenericResponse<String>
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500 error restarting the job
     */
    @PUT
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/{jobId}/restart")
    @GZIP
    public GenericResponse<String> restart(@PathParam("jobId") String jobId) {
        
        GenericResponse<String> response = new GenericResponse<>();
        
        // Find all potential running jobs
        MapReduceInfoResponseList list = mapReduceState.findById(jobId);
        // Should contain zero or one job
        if (list.getResults().size() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_MAPREDUCE_OBJECT_MATCH);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        } else if (list.getResults().size() > 1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_MAPREDUCE_OBJECT_MATCHES);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        } else {
            MapReduceInfoResponse thisJob = list.getResults().get(0);
            // Call cancel for this job. This will kill any running jobs and remove the results directory
            cancel(jobId);
            // Now re-submit this job after adding the JOB_ID to the runtime parameters to signal that this job has been restarted
            String jobName = thisJob.getJobName();
            // Now call submit
            return submit(jobName, thisJob.getRuntimeParameters() + PARAMETER_SEPARATOR + JOB_ID + PARAMETER_NAME_VALUE_SEPARATOR + jobId);
        }
        
    }
    
    /**
     * Returns status of a job with the given jobId
     *
     * @param jobId
     * @return nsa.datawave.webservice.results.mr.MapReduceInfoResponseList
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/{jobId}/list")
    @GZIP
    public MapReduceInfoResponseList list(@PathParam("jobId") String jobId) {
        MapReduceInfoResponseList response = mapReduceState.findById(jobId);
        if (null == response || null == response.getResults() || response.getResults().size() == 0) {
            if (null == response)
                response = new MapReduceInfoResponseList();
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        }
        if (response.getResults().size() > 1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_MAPREDUCE_OBJECT_MATCHES);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        }
        return response;
    }
    
    /**
     * Returns the contents of a result file. The list of resulting output files from the MapReduce job is listed in the response object of the status
     * operation.
     *
     * @param jobId
     * @param fileName
     * @return file contents
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500
     */
    @GET
    @Produces("*/*")
    @javax.ws.rs.Path("/{jobId}/getFile/{fileName}")
    @GZIP
    public StreamingOutput getResultFile(@PathParam("jobId") String jobId, @PathParam("fileName") String fileName) {
        MapReduceInfoResponseList response = list(jobId);
        MapReduceInfoResponse result = response.getResults().get(0);
        String hdfs = result.getHdfs();
        String resultsDir = result.getResultsDirectory();
        final FileSystem fs = getFS(hdfs, response);
        final Path resultFile = new Path(resultsDir, fileName);
        
        FSDataInputStream fis;
        try {
            if (!fs.exists(resultFile) || !fs.isFile(resultFile)) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.FILE_NOT_FOUND, MessageFormat.format("{0} at path {1}", fileName,
                                resultsDir));
                response.addException(qe);
                throw new NotFoundException(qe, response);
            }
            
            fis = fs.open(resultFile);
        } catch (IOException e1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.RESULT_FILE_ACCESS_ERROR, e1, MessageFormat.format("{0}",
                            resultFile.toString()));
            log.error(qe);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        }
        
        // Make a final reference to the fis for referencing inside the inner class
        final FSDataInputStream fiz = fis;
        return new StreamingOutput() {
            
            private Logger log = Logger.getLogger(this.getClass());
            
            @Override
            public void write(java.io.OutputStream output) throws IOException, WebApplicationException {
                byte[] buf = new byte[BUFFER_SIZE];
                int read;
                try {
                    read = fiz.read(buf);
                    while (read != -1) {
                        output.write(buf, 0, read);
                        read = fiz.read(buf);
                    }
                    
                } catch (Exception e) {
                    log.error("Error writing result file to output", e);
                    throw new WebApplicationException(e);
                } finally {
                    try {
                        if (null != fiz)
                            fiz.close();
                    } catch (IOException e) {
                        log.error("Error closing FSDataInputStream for file: " + resultFile.toString(), e);
                    }
                    try {
                        if (null != fs)
                            fs.close();
                    } catch (IOException e) {
                        log.error("Error closing HDFS client", e);
                    }
                }
            }
            
        };
    }
    
    /**
     * Returns the a tar file where each tar entry is a result file.
     *
     * @param jobId
     * @return tar file
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500
     */
    @GET
    @Produces("*/*")
    @javax.ws.rs.Path("/{jobId}/getAllFiles")
    @GZIP
    public StreamingOutput getResultFiles(@Required("jobId") @PathParam("jobId") final String jobId, @Context HttpServletResponse httpResponse) {
        MapReduceInfoResponseList response = list(jobId);
        MapReduceInfoResponse result = response.getResults().get(0);
        String hdfs = result.getHdfs();
        String resultsDir = result.getResultsDirectory();
        final FileSystem fs = getFS(hdfs, response);
        
        final Path jobDirectory = new Path(resultsDir);
        final int jobDirectoryPathLength = jobDirectory.toUri().getPath().length();
        try {
            if (!fs.exists(jobDirectory) || !fs.getFileStatus(jobDirectory).isDirectory()) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.JOB_DIRECTORY_NOT_FOUND, MessageFormat.format("{0} at path {1}",
                                jobId, jobDirectory));
                response.addException(qe);
                throw new NotFoundException(qe, response);
            }
        } catch (IOException e1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.RESULT_DIRECTORY_ACCESS_ERROR, e1, MessageFormat.format("{0}", resultsDir));
            log.error(qe);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        }
        
        // Get the children
        List<FileStatus> resultFiles = new ArrayList<>();
        try {
            // recurse through the directory to find all files
            Queue<FileStatus> fileQueue = new LinkedList<>();
            fileQueue.add(fs.getFileStatus(jobDirectory));
            while (!fileQueue.isEmpty()) {
                FileStatus currentFileStatus = fileQueue.remove();
                if (currentFileStatus.isFile()) {
                    resultFiles.add(currentFileStatus);
                } else {
                    FileStatus[] dirList = fs.listStatus(currentFileStatus.getPath());
                    for (FileStatus fileStatus : dirList) {
                        fileQueue.add(fileStatus);
                    }
                }
            }
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.DFS_DIRECTORY_LISTING_ERROR, e, MessageFormat.format("directory: {0}", resultsDir));
            log.error(qe);
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        String filename = jobId + "-files.tar";
        httpResponse.addHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        
        // Make final references for use in anonymous class
        final List<FileStatus> paths = resultFiles;
        return new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                TarArchiveOutputStream tos = new TarArchiveOutputStream(output);
                tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                try {
                    for (FileStatus fileStatus : paths) {
                        if (fileStatus.isDirectory())
                            continue;
                        // The archive entry will be started when the first (and possibly only) chunk is
                        // written out. It is done this way because we need to know the size of the file
                        // for the archive entry, and don't want to scan twice to get that info (once
                        // here and again in streamFile).
                        String fileName = fileStatus.getPath().toUri().getPath().substring(jobDirectoryPathLength + 1);
                        TarArchiveEntry entry = new TarArchiveEntry(jobId + "/" + fileName, false);
                        entry.setSize(fileStatus.getLen());
                        tos.putArchiveEntry(entry);
                        FSDataInputStream fis = fs.open(fileStatus.getPath());
                        byte[] buf = new byte[BUFFER_SIZE];
                        int read;
                        try {
                            read = fis.read(buf);
                            while (read != -1) {
                                tos.write(buf, 0, read);
                                read = fis.read(buf);
                            }
                        } catch (Exception e) {
                            log.error("Error writing result file to output", e);
                            throw new WebApplicationException(e);
                        } finally {
                            try {
                                if (null != fis)
                                    fis.close();
                            } catch (IOException e) {
                                log.error("Error closing FSDataInputStream for file: " + fileStatus.getPath().getName(), e);
                            }
                        }
                        tos.closeArchiveEntry();
                    }
                    tos.finish();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    try {
                        if (null != tos)
                            tos.close();
                    } catch (IOException ioe) {
                        log.error("Error closing TarArchiveOutputStream", ioe);
                    }
                    try {
                        if (null != fs)
                            fs.close();
                    } catch (IOException ioe) {
                        log.error("Error closing HDFS client", ioe);
                    }
                }
            }
        };
        
    }
    
    /**
     * List the status of all jobs for the current user
     *
     * @return nsa.datawave.webservice.results.mr.MapReduceInfoResponseList
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500 error restarting the job
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/list")
    @GZIP
    public MapReduceInfoResponseList list() {
        return mapReduceState.find();
    }
    
    /**
     * Removes the MapReduce entry and associated data
     * 
     * @param jobid
     * @return nsa.datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @HTTP 200 success
     * @HTTP 404 if jobId is invalid or cannot be found
     * @HTTP 500 error removing the job
     */
    @DELETE
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @javax.ws.rs.Path("/{jobId}/remove")
    @GZIP
    public VoidResponse remove(@PathParam("jobId") String jobId) {
        VoidResponse response = new VoidResponse();
        // Call cancel which will kill any running jobs and remove the results directory in HDFS.
        cancel(jobId);
        // Remove the working directory from HDFS
        MapReduceInfoResponseList list = list(jobId);
        MapReduceInfoResponse result = list.getResults().get(0);
        String hdfs = result.getHdfs();
        String wdir = result.getWorkingDirectory();
        Path p = new Path(wdir);
        try {
            FileSystem fs = getFS(hdfs, response);
            if (fs.exists(p) && !fs.delete(p, true)) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.WORKING_DIRECTORY_DELETE_ERROR, MessageFormat.format("{0}", wdir));
                log.error(qe);
                response.addException(qe);
                throw new NotFoundException(qe, response);
            }
        } catch (IOException e) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.WORKING_DIRECTORY_DELETE_ERROR, e, MessageFormat.format("{0}", wdir));
            log.error(qe);
            throw new NotFoundException(qe, response);
        }
        // Remove any persisted state information
        try {
            mapReduceState.remove(jobId);
        } catch (QueryException e) {
            log.error("Error removing job state information", e);
            response.addException(e.getBottomQueryException());
            throw new DatawaveWebApplicationException(e, response);
        }
        
        return response;
    }
    
    private FileSystem getFS(String hdfs, BaseResponse response) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", hdfs);
        // Override default buffer size (4K) to 16K
        conf.setInt("io.file.buffer.size", 16384);
        final FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            return fs;
        } catch (IOException e1) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.HDFS_CONNECTION_ERROR, e1, MessageFormat.format("Location: {0}", hdfs));
            log.error(qe);
            response.addException(qe);
            throw new NotFoundException(qe, response);
        }
    }
    
    protected void canRunJob(Principal principal, MultivaluedMap<String,String> queryParameters, List<String> requiredRoles, List<String> requiredAuths)
                    throws UnauthorizedQueryException {
        if (principal instanceof DatawavePrincipal == false) {
            throw new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED, "Principal must be DatawavePrincipal");
        }
        DatawavePrincipal datawavePrincipal = (DatawavePrincipal) principal;
        if (requiredRoles != null && requiredRoles.size() > 0) {
            Set<String> usersRoles = new HashSet<>();
            Map<String,Collection<String>> userRolesMap = datawavePrincipal.getUserRolesMap();
            if (userRolesMap.size() == 1) {
                usersRoles.addAll(userRolesMap.values().iterator().next());
            } else if (userRolesMap.size() > 1) {
                String userDN = datawavePrincipal.getUserDN();
                for (Entry<String,Collection<String>> entry : userRolesMap.entrySet()) {
                    if (entry.getKey().contains(userDN)) {
                        usersRoles.addAll(entry.getValue());
                        break;
                    }
                }
            }
            if (usersRoles.containsAll(requiredRoles) == false) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED, MessageFormat.format("Requires the following roles: {0}",
                                requiredRoles));
            }
        }
        
        if (null != queryParameters) {
            if (requiredAuths != null && requiredAuths.size() > 0) {
                String authsString = queryParameters.getFirst("auths");
                List<String> authorizations = AuthorizationsUtil.splitAuths(authsString);
                if (authorizations.containsAll(requiredAuths) == false) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.JOB_EXECUTION_UNAUTHORIZED, MessageFormat.format(
                                    "Requires the following auths: {0}", requiredAuths));
                }
            }
        }
    }
}
