package datawave.microservice.query.mapreduce.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.InputFormat;
import org.springframework.boot.context.properties.ConfigurationProperties;

import datawave.microservice.query.config.ThreadPoolTaskExecutorProperties;

@ConfigurationProperties(prefix = MapReduceQueryProperties.PREFIX)
public class MapReduceQueryProperties {
    
    public static final String PREFIX = "datawave.query.mapreduce";
    public static final String JOB_NAME = "jobName";
    public static final String PARAMETERS = "parameters";
    public static final String QUERY_ID = "queryId";
    public static final String FORMAT = "format";
    public static final String OUTPUT_TABLE_NAME = "outputTableName";
    public static final String OUTPUT_FORMAT = "outputFormat";
    
    private String callbackServletURL;
    
    private String mapReduceBaseDirectory;
    
    private boolean restrictInputFormats;
    
    private List<Class<? extends InputFormat<?,?>>> validInputFormats;
    
    private Map<String,MapReduceJobProperties> jobs;
    
    private List<String> fsConfigResources;
    
    // The amount of time to wait for the lock to be acquired
    private long lockWaitTime = TimeUnit.SECONDS.toMillis(5);
    
    private TimeUnit lockWaitTimeUnit = TimeUnit.MILLISECONDS;
    
    // The amount of time that the lock will be held before being automatically released
    private long lockLeaseTime = TimeUnit.SECONDS.toMillis(30);
    
    private TimeUnit lockLeaseTimeUnit = TimeUnit.MILLISECONDS;
    
    private String mapReduceQueryServiceName = "mrquery";
    
    private ThreadPoolTaskExecutorProperties executor = new ThreadPoolTaskExecutorProperties(8, 16, 100, "mapReduceCall-");
    
    public String getCallbackServletURL() {
        return callbackServletURL;
    }
    
    public void setCallbackServletURL(String callbackServletURL) {
        this.callbackServletURL = callbackServletURL;
    }
    
    public String getMapReduceBaseDirectory() {
        return mapReduceBaseDirectory;
    }
    
    public void setMapReduceBaseDirectory(String mapReduceBaseDirectory) {
        this.mapReduceBaseDirectory = mapReduceBaseDirectory;
    }
    
    public boolean isRestrictInputFormats() {
        return restrictInputFormats;
    }
    
    public void setRestrictInputFormats(boolean restrictInputFormats) {
        this.restrictInputFormats = restrictInputFormats;
    }
    
    public List<Class<? extends InputFormat<?,?>>> getValidInputFormats() {
        return validInputFormats;
    }
    
    public void setValidInputFormats(List<Class<? extends InputFormat<?,?>>> validInputFormats) {
        this.validInputFormats = validInputFormats;
    }
    
    public Map<String,MapReduceJobProperties> getJobs() {
        return jobs;
    }
    
    public void setJobs(Map<String,MapReduceJobProperties> jobs) {
        this.jobs = jobs;
    }
    
    public List<String> getFsConfigResources() {
        return fsConfigResources;
    }
    
    public void setFsConfigResources(List<String> fsConfigResources) {
        this.fsConfigResources = fsConfigResources;
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTimeUnit.toMillis(lockWaitTime);
    }
    
    public void setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    public TimeUnit getLockWaitTimeUnit() {
        return lockWaitTimeUnit;
    }
    
    public void setLockWaitTimeUnit(TimeUnit lockWaitTimeUnit) {
        this.lockWaitTimeUnit = lockWaitTimeUnit;
    }
    
    public long getLockLeaseTime() {
        return lockLeaseTime;
    }
    
    public long getLockLeaseTimeMillis() {
        return lockLeaseTimeUnit.toMillis(lockLeaseTime);
    }
    
    public void setLockLeaseTime(long lockLeaseTime) {
        this.lockLeaseTime = lockLeaseTime;
    }
    
    public TimeUnit getLockLeaseTimeUnit() {
        return lockLeaseTimeUnit;
    }
    
    public void setLockLeaseTimeUnit(TimeUnit lockLeaseTimeUnit) {
        this.lockLeaseTimeUnit = lockLeaseTimeUnit;
    }
    
    public String getMapReduceQueryServiceName() {
        return mapReduceQueryServiceName;
    }
    
    public void setMapReduceQueryServiceName(String mapReduceQueryServiceName) {
        this.mapReduceQueryServiceName = mapReduceQueryServiceName;
    }
    
    public ThreadPoolTaskExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ThreadPoolTaskExecutorProperties executor) {
        this.executor = executor;
    }
}
