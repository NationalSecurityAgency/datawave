package datawave.microservice.query.mapreduce.jobs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.mapreduce.config.MapReduceJobProperties;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.mapreduce.status.MapReduceQueryStatus;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public abstract class MapReduceJob {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected final MapReduceQueryProperties mapReduceQueryProperties;
    
    protected final MapReduceJobProperties mapReduceJobProperties;
    
    public MapReduceJob(MapReduceQueryProperties mapReduceQueryProperties) {
        this.mapReduceQueryProperties = mapReduceQueryProperties;
        this.mapReduceJobProperties = mapReduceQueryProperties.getJobs().get(this.getClass().getSimpleName());
    }
    
    public String createId(DatawaveUserDetails currentUser) {
        return UUID.randomUUID().toString();
    }
    
    public void validateParameters(MultiValueMap<String,String> parameters) throws BadRequestQueryException {
        // Validate the required runtime parameters exist
        if (null != mapReduceJobProperties.getRequiredRuntimeParameters() && !mapReduceJobProperties.getRequiredRuntimeParameters().isEmpty()
                        && null != parameters && !parameters.isEmpty()) {
            // Loop over the required runtime parameter names and make sure an entry exists in the method parameter
            for (String parameter : mapReduceJobProperties.getRequiredRuntimeParameters().keySet()) {
                if (!parameters.containsKey(parameter) || StringUtils.isBlank(parameters.getFirst(parameter))) {
                    log.error("Required runtime parameter '" + parameter + "' must be set");
                    throw new BadRequestQueryException("Required runtime parameter '" + parameter + "' must be set.", HttpStatus.SC_BAD_REQUEST + "-1");
                }
            }
        }
        
        parameters.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        parameters.remove(AuditParameters.USER_DN);
        parameters.remove(AuditParameters.QUERY_AUDIT_TYPE);
    }
    
    /**
     * @param conf
     *            config
     * @return HDFS FileSystem
     * @throws IOException
     *             for IOException
     */
    protected final FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }
    
    /**
     * Common code to setup distributed cache and classpath for the job
     *
     * @param id
     *            the job id
     * @param job
     *            the job
     * @param baseDir
     *            the base directory
     * @param jobDir
     *            the job directory
     * @throws Exception
     *             if something goes wrong
     */
    protected void prepareClasspath(String id, Job job, Path baseDir, Path jobDir) throws Exception {
        FileSystem fs = getFileSystem(job.getConfiguration());
        // Create the base directory
        if (!fs.exists(baseDir)) {
            if (!fs.mkdirs(baseDir)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", baseDir.toString()));
            }
        }
        
        // Create the job directory
        if (!fs.exists(jobDir)) {
            if (!fs.mkdirs(jobDir)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", jobDir.toString()));
            }
        }
        
        // Create the job classpath directory
        Path classpath = new Path(jobDir, "classpath");
        if (!fs.exists(classpath)) {
            if (!fs.mkdirs(classpath)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", classpath.toString()));
            }
        }
        
        exportSystemProperties(id, job, fs, classpath);
    }
    
    protected void addSingleFile(File source, Path destination, String jobId, Job job, FileSystem fs) throws IOException {
        Path jarPath = new Path(source.getAbsolutePath());
        try {
            fs.copyFromLocalFile(false, false, jarPath, destination);
        } catch (IOException e) {
            // If the file already exists, ignore error
            if (!e.getMessage().endsWith("already exists")) {
                throw e;
            }
        }
        log.trace("Adding {} to the classpath for job {}.", jarPath, jobId);
        job.addFileToClassPath(destination);
    }
    
    protected void exportSystemProperties(String id, Job job, FileSystem fs, Path classpath) {
        Properties systemProperties = new Properties();
        systemProperties.putAll(System.getProperties());
        if (mapReduceJobProperties.getJobSystemProperties() != null) {
            systemProperties.putAll(mapReduceJobProperties.getJobSystemProperties());
        }
        writeProperties(id, job, fs, classpath, systemProperties);
    }
    
    protected void writeProperties(String id, Job job, FileSystem fs, Path classpath, Properties properties) {
        File f = null;
        try {
            f = File.createTempFile(id, ".properties");
            try (FileOutputStream fos = new FileOutputStream(f)) {
                properties.store(fos, "");
            }
            addSingleFile(f, new Path(classpath, "application.properties"), id, job, fs);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (f != null) {
                f.delete();
            }
        }
    }
    
    public void initializeConfiguration(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus, Job job) throws Exception {
        
        if (StringUtils.isEmpty(mapReduceJobProperties.getHdfsUri())) {
            throw new IllegalArgumentException("HDFS URI must be set");
        }
        job.getConfiguration().set("fs.defaultFS", mapReduceJobProperties.getHdfsUri());
        
        if (StringUtils.isEmpty(mapReduceJobProperties.getJobTracker())) {
            throw new IllegalArgumentException("Job Tracker must be set");
        }
        job.getConfiguration().set("mapreduce.jobtracker.address", mapReduceJobProperties.getJobTracker());
        
        if (StringUtils.isEmpty(mapReduceJobProperties.getJobJarName())) {
            throw new IllegalArgumentException("Job jar name must be set");
        }
        job.getConfiguration().set("mapreduce.job.jar", mapReduceJobProperties.getJobJarName());
        
        // Set the job.end.notification.url parameter for the job. The MapReduce framework will call back to this servlet
        // when the MapReduce job succeeds/fails. For deploying on a cluster, the job.end.retry.attempts and job.end.retry.interval
        // parameters need to be set in the Hadoop mapred config file.
        job.getConfiguration().set("mapreduce.job.end-notification.url",
                        mapReduceQueryProperties.getCallbackServletURL() + "?jobId=$jobId&jobStatus=$jobStatus");
        
        Path baseDir = new Path(mapReduceQueryProperties.getMapReduceBaseDirectory());
        
        // Create a directory path for this job
        Path jobDir = new Path(baseDir, mapReduceQueryStatus.getId());
        mapReduceQueryStatus.setWorkingDirectory(jobDir.toString());
        
        // Create a results directory path
        mapReduceQueryStatus.setResultsDirectory(new Path(jobDir, "results").toString());
        
        // Set up the classpath
        prepareClasspath(mapReduceQueryStatus.getId(), job, baseDir, jobDir);
        
        // Add any configuration properties set in the config
        for (Map.Entry<String,String> entry : mapReduceJobProperties.getJobConfigurationProperties().entrySet()) {
            job.getConfiguration().set(entry.getKey(), entry.getValue());
        }
        
        _initializeConfiguration(queryLogicFactory, accumuloConnectionFactory, mapReduceQueryStatus, job, jobDir);
    }
    
    // Subclasses will override this method
    protected void _initializeConfiguration(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus, Job job, Path jobDir) throws Exception {
        
    }
    
    public MapReduceJobProperties getMapReduceJobProperties() {
        return mapReduceJobProperties;
    }
}
