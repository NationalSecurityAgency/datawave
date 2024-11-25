package datawave.microservice.audit.auditors.accumulo;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

/**
 * An implementation for {@link Auditor}, which writes audit messages to Accumulo.
 */
public class AccumuloAuditor implements Auditor {
    
    private static Logger log = LoggerFactory.getLogger(AccumuloAuditor.class);
    
    private SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
    
    private String tableName;
    
    private AccumuloClient accumuloClient;
    
    private ConcurrentHashMap<String,Long> auditTimers = new ConcurrentHashMap<>();
    
    public AccumuloAuditor(String tableName, AccumuloClient client) {
        this.tableName = tableName;
        this.accumuloClient = client;
        init();
    }
    
    private void init() {
        try {
            if (!accumuloClient.tableOperations().exists(tableName))
                accumuloClient.tableOperations().create(tableName);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Unable to create audit table.", e);
        } catch (TableExistsException e) {
            log.warn("Accumulo Audit Table [{}] already exists.", tableName, e);
        }
    }
    
    @Override
    public void audit(AuditParameters msg) throws Exception {
        String auditId = msg.getAuditId();
        if (auditId == null) {
            auditId = UUID.randomUUID().toString();
        }
        
        // save the start time of the audit call
        auditTimers.put(auditId, System.currentTimeMillis());
        try {
            if (!msg.getAuditType().equals(AuditType.NONE)) {
                try (BatchWriter writer = accumuloClient.createBatchWriter(tableName,
                                new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1))) {
                    Mutation m = new Mutation(formatter.format(msg.getQueryDate()));
                    m.put(new Text(msg.getUserDn()), new Text(""), msg.getColviz(), new Value(msg.toString().getBytes(UTF_8)));
                    writer.addMutation(m);
                    writer.flush();
                }
            }
        } finally {
            auditTimers.remove(auditId);
        }
    }
    
    public ConcurrentHashMap<String,Long> getAuditTimers() {
        return auditTimers;
    }
}
