package datawave.microservice.audit.auditors.accumulo;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * An implementation for {@link Auditor}, which writes audit messages to Accumulo.
 */
public class AccumuloAuditor implements Auditor {
    
    private static Logger log = LoggerFactory.getLogger(AccumuloAuditor.class);
    
    private SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
    
    private String tableName;
    
    private Connector connector;
    
    public AccumuloAuditor(String tableName, Connector connector) {
        this.tableName = tableName;
        this.connector = connector;
        init();
    }
    
    private void init() {
        try {
            if (!connector.tableOperations().exists(tableName))
                connector.tableOperations().create(tableName);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Unable to create audit table.", e);
        } catch (TableExistsException e) {
            log.warn("Accumulo Audit Table [{}] already exists.", tableName, e);
        }
    }
    
    @Override
    public void audit(AuditParameters msg) throws Exception {
        if (!msg.getAuditType().equals(AuditType.NONE)) {
            try (BatchWriter writer = connector.createBatchWriter(tableName,
                            new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1))) {
                Mutation m = new Mutation(formatter.format(msg.getQueryDate()));
                m.put(new Text(msg.getUserDn()), new Text(""), msg.getColviz(), new Value(msg.toString().getBytes()));
                writer.addMutation(m);
                writer.flush();
            }
        }
    }
}
