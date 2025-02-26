package datawave.microservice.query.mapreduce.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.util.MultiValueMap;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.mapreduce.config.MapReduceQueryProperties;
import datawave.microservice.query.mapreduce.status.MapReduceQueryStatus;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.common.audit.Auditor;

public class OozieJob extends MapReduceJob {
    
    public static final String WORKFLOW = "workFlow";
    public static final String OOZIE_CLIENT = "oozie.client";
    public static final String JOB_TRACKER = "jobTracker";
    public static final String NAME_NODE = "nameNode";
    public static final String QUEUE_NAME = "queueName";
    public static final String OUTPUT_WF_ID = "outputWfId";
    public static final String OUT_DIR = "outDir";
    
    protected Auditor.AuditType auditType = Auditor.AuditType.ACTIVE;
    
    public OozieJob(MapReduceQueryProperties mapReduceQueryProperties) {
        super(mapReduceQueryProperties);
    }
    
    @Override
    public String createId(DatawaveUserDetails currentUser) {
        return String.join("_", ProxiedEntityUtils.getShortName(currentUser.getPrimaryUser().getName()), UUID.randomUUID().toString());
    }
    
    @Override
    public void initializeConfiguration(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus, Job job) throws Exception {
        throw new NotImplementedException();
    }
    
    public void initializeOozieConfiguration(String jobId, Properties oozieProperties, MultiValueMap<String,String> queryParameters) throws Exception {
        
        // set the configured job properties
        for (Map.Entry<String,String> entry : getMapReduceJobProperties().getJobConfigurationProperties().entrySet()) {
            oozieProperties.setProperty(entry.getKey(), entry.getValue());
        }
        
        // set the user-defined job properties
        for (String key : queryParameters.keySet()) {
            oozieProperties.setProperty(key, queryParameters.getFirst(key));
        }
        
        oozieProperties.setProperty(JOB_TRACKER, getMapReduceJobProperties().getJobTracker());
        oozieProperties.setProperty(NAME_NODE, getMapReduceJobProperties().getHdfsUri());
        oozieProperties.setProperty(QUEUE_NAME, getMapReduceJobProperties().getQueueName());
        oozieProperties.setProperty(OUTPUT_WF_ID, jobId);
        
        _initializeOozieConfiguration(jobId, oozieProperties, queryParameters);
    }
    
    @Override
    protected void _initializeConfiguration(QueryLogicFactory queryLogicFactory, AccumuloConnectionFactory accumuloConnectionFactory,
                    MapReduceQueryStatus mapReduceQueryStatus, Job job, Path jobDir) throws Exception {
        throw new NotImplementedException();
    }
    
    // Subclasses will override this method
    protected void _initializeOozieConfiguration(String jobId, Properties oozieProperties, MultiValueMap<String,String> queryParameters) throws Exception {
        
    }
    
    public void validateWorkflowParameter(Properties oozieConf) {
        // Validate the required runtime parameters exist
        if (null != getMapReduceJobProperties().getRequiredRuntimeParameters() && !getMapReduceJobProperties().getRequiredRuntimeParameters().isEmpty()) {
            // Loop over the required runtime parameter names and make sure an entry exists in the method parameter
            for (String parameter : getMapReduceJobProperties().getRequiredRuntimeParameters().keySet()) {
                if (!oozieConf.containsKey(parameter) || StringUtils.isBlank((String) oozieConf.get(parameter)))
                    throw new IllegalArgumentException("Required runtime parameter '" + parameter + "' must be set");
            }
        }
        
        // Validate the required parameters exist
        if (null != getMapReduceJobProperties().getRequiredParameters() && !getMapReduceJobProperties().getRequiredParameters().isEmpty()) {
            // Loop over the required parameter names and make sure an entry exists in the queryParameters
            for (String parameter : getMapReduceJobProperties().getRequiredParameters().keySet()) {
                if (!oozieConf.containsKey(parameter) || StringUtils.isBlank((String) oozieConf.get(parameter)))
                    throw new IllegalArgumentException("Required parameter '" + parameter + "' must be set");
            }
        }
        
    }
    
    public Auditor.AuditType getAuditType() {
        return auditType;
    }
    
    public void setAuditType(Auditor.AuditType auditType) {
        this.auditType = auditType;
    }
    
    public String getQuery(MultiValueMap<String,String> queryParameters) {
        return "";
    }
    
    public List<String> getSelectors(MultiValueMap<String,String> queryParameters) {
        return new ArrayList<>();
    }
}
