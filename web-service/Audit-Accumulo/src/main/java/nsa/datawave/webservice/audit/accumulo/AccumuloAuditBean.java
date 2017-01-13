package nsa.datawave.webservice.audit.accumulo;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBException;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;

import nsa.datawave.webservice.common.audit.AuditParameters;
import nsa.datawave.webservice.common.audit.Auditor;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@LocalBean
@Stateless
@RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
public class AccumuloAuditBean implements Auditor {
    
    private Logger log = Logger.getLogger(this.getClass());
    private SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    private static final String TABLE_NAME = "QueryAuditTable";
    
    @PostConstruct
    public void init() {
        Connector connector = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connector = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            if (!connector.tableOperations().exists(TABLE_NAME))
                connector.tableOperations().create(TABLE_NAME);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new EJBException("Unable to create audit table");
        } finally {
            if (connector != null) {
                try {
                    connectionFactory.returnConnection(connector);
                } catch (Exception e) {
                    log.error("Unable to return accumulo connection", e);
                }
            }
        }
    }
    
    @Override
    public void audit(AuditParameters msg) throws Exception {
        if (!msg.getAuditType().equals(AuditType.NONE)) {
            Connector connector = null;
            BatchWriter writer = null;
            try {
                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                connector = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
                writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L)
                                .setMaxWriteThreads(1));
                Mutation m = new Mutation(formatter.format(msg.getQueryDate()));
                m.put(new Text(msg.getUserDn()), new Text(""), msg.getColviz(), new Value(msg.toString().getBytes()));
                writer.addMutation(m);
                writer.flush();
            } finally {
                try {
                    connectionFactory.returnConnection(connector);
                    if (null != writer)
                        writer.close();
                } catch (Exception ex) {
                    log.error("Error returning connection", ex);
                }
            }
        }
    }
    
}
