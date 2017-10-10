package datawave.microservice.audit.accumulo;

import datawave.microservice.audit.accumulo.config.AccumuloAuditProperties;
import datawave.microservice.audit.accumulo.config.AccumuloAuditProperties.Accumulo;
import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.common.Auditor;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class AccumuloAuditor implements Auditor {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    private SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
    
    private AccumuloAuditProperties accumuloAuditProperties;
    
    private Connector connector;
    
    public AccumuloAuditor(AccumuloAuditProperties accumuloAuditProperties) {
        this.accumuloAuditProperties = accumuloAuditProperties;
        init();
    }
    
    private void init() {
        final Accumulo accumulo = accumuloAuditProperties.getAccumuloConfig();
        final BaseConfiguration baseConfiguration = new BaseConfiguration();
        baseConfiguration.setDelimiterParsingDisabled(true); // Silence warnings about multi-value properties
        baseConfiguration.setProperty("instance.name", accumulo.getInstanceName());
        baseConfiguration.setProperty("instance.zookeeper.host", accumulo.getZookeepers());
        final ClientConfiguration clientConfiguration = new ClientConfiguration(baseConfiguration);
        final Instance instance = new ZooKeeperInstance(clientConfiguration);
        try {
            connector = instance.getConnector(accumulo.getUsername(), new PasswordToken(accumulo.getPassword()));
            if (!connector.tableOperations().exists(accumuloAuditProperties.getTableName()))
                connector.tableOperations().create(accumuloAuditProperties.getTableName());
        } catch (AccumuloException | AccumuloSecurityException e) {
            if (connector != null)
                log.error("Unable to create audit table.", e);
            else
                log.error("Unable to contact Accumulo.", e);
        } catch (TableExistsException e) {
            log.warn("Accumulo Audit Table [" + accumuloAuditProperties.getTableName() + "] already exists.", e);
        }
    }
    
    @Override
    public void audit(AuditParameters msg) throws Exception {
        if (!msg.getAuditType().equals(AuditType.NONE)) {
            BatchWriter writer = connector.createBatchWriter(accumuloAuditProperties.getTableName(), new BatchWriterConfig()
                            .setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1));
            Mutation m = new Mutation(formatter.format(msg.getQueryDate()));
            m.put(new Text(msg.getUserDn()), new Text(""), msg.getColviz(), new Value(msg.toString().getBytes()));
            writer.addMutation(m);
            writer.flush();
        }
    }
}
