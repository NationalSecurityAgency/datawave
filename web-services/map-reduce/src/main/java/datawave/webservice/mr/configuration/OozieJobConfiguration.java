package datawave.webservice.mr.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.Auditor;

public class OozieJobConfiguration extends MapReduceJobConfiguration {

    private Logger log = LoggerFactory.getLogger(getClass());

    protected Auditor.AuditType auditType = Auditor.AuditType.ACTIVE;

    @Override
    public void initializeConfiguration(String jobId, Job job, Map<String,String> runtimeParameters, DatawavePrincipal userPrincipal) throws Exception {
        throw new NotImplementedException();
    }

    public void initializeOozieConfiguration(String jobId, Properties oozieConf, MultivaluedMap<String,String> queryParameters) throws Exception {

        Map<String,Object> jobProperties = getJobConfigurationProperties();

        for (Map.Entry<String,Object> entry : jobProperties.entrySet()) {
            if (entry.getValue() instanceof String) {
                oozieConf.setProperty(entry.getKey(), (String) entry.getValue());
            }
        }

        oozieConf.setProperty(OozieJobConstants.JOB_TRACKER_PARAM, getJobTracker());
        oozieConf.setProperty(OozieJobConstants.NAME_NODE_PARAM, getHdfsUri());
        oozieConf.setProperty(OozieJobConstants.QUEUE_NAME_PARAM, getQueueName());
        oozieConf.setProperty(OozieJobConstants.OUTPUT_WF_ID_PARAM, jobId);

        String parameters = queryParameters.getFirst(OozieJobConstants.PARAMETERS);

        // Add any configuration properties set in the config
        for (Map.Entry<String,Object> entry : this.getJobConfigurationProperties().entrySet()) {
            if (entry.getValue() instanceof String) {
                oozieConf.setProperty(entry.getKey(), (String) entry.getValue());
            }
        }

        // Parse the parameters
        Map<String,String> runtimeParameters = new HashMap<>();
        if (null != parameters) {
            String[] param = parameters.split(OozieJobConstants.PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(OozieJobConstants.PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    String name = parts[0];
                    String value = parts[1];
                    runtimeParameters.put(name, value);
                    oozieConf.setProperty(name, value);
                }
            }
        }

        for (String key : queryParameters.keySet()) {
            oozieConf.setProperty(key, queryParameters.getFirst(key));
        }

        _initializeOozieConfiguration(jobId, oozieConf, queryParameters);
    }

    @Override
    protected void _initializeConfiguration(Job job, Path jobDir, String jobId, Map<String,String> runtimeParameters, DatawavePrincipal serverPrincipal)
                    throws Exception {
        throw new NotImplementedException();
    }

    // Subclasses will override this method
    protected void _initializeOozieConfiguration(String jobId, Properties oozieConf, MultivaluedMap<String,String> queryParameters) throws Exception {

    }

    public void setAuditType(Auditor.AuditType auditType) {
        this.auditType = auditType;
    }

    public Auditor.AuditType getAuditType() {
        return auditType;
    }

    public MultivaluedMap<String,String> getAuditParameters(MultivaluedMap<String,String> queryParameters, Properties oozieConf,
                    Collection<String> requiredAuditParams) {

        return queryParameters;
    }

    public List<String> getSelectors(MultivaluedMap<String,String> queryParameters, Properties oozieConf) {
        return new ArrayList<>();
    }

    public void validateWorkflowParameter(Properties oozieConf, MapReduceConfiguration mapReduceConfiguration) {
        // Validate the required runtime parameters exist
        if (null != this.requiredRuntimeParameters && !this.requiredRuntimeParameters.isEmpty()) {
            // Loop over the required runtime parameter names and make sure an entry exists in the method parameter
            for (String parameter : this.requiredRuntimeParameters.keySet()) {
                if (!oozieConf.containsKey(parameter))
                    throw new IllegalArgumentException("Required runtime parameter '" + parameter + "' must be set");
            }
        }

        // Validate the required parameters exist
        if (null != this.requiredParameters && !this.requiredParameters.isEmpty()) {
            // Loop over the required parameter names and make sure an entry exists in the queryParameters
            for (String parameter : this.requiredParameters.keySet()) {
                if (!oozieConf.containsKey(parameter))
                    throw new IllegalArgumentException("Required parameter '" + parameter + "' must be set");
            }
        }

    }
}
