package nsa.datawave.webservice.common.audit;

import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.ejb.EJBException;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Message;

import nsa.datawave.webservice.common.audit.Auditor.AuditType;
import org.apache.log4j.Logger;

public abstract class AuditReceiverBase {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private AuditParameters auditParameters;
    
    // Allow anyone to call onMessage. This seems to be required when the bean is in a security domain,
    // but since this is not exposed as an EJB we should be ok.
    @PermitAll
    public void onMessage(Message message) {
        try {
            try {
                Map<String,Object> map = message.getBody(Map.class);
                onMessage(map);
            } catch (JMSException e) {
                log.warn("Nothing processed. Message was not a Map: " + message, e);
            }
        } catch (Exception e) {
            throw new EJBException(e);
        }
    }
    
    public void onMessage(Map<String,Object> msg) {
        try {
            // AUTO_ACKNOWLEDGE is the default activation config property. If you need the client to
            // acknowledge the message, do it in the subclass.
            auditParameters.clear();
            AuditParameters ap = auditParameters.fromMap(msg);
            if (!ap.getAuditType().equals(AuditType.NONE)) {
                getAuditor().audit(ap);
            }
        } catch (Exception e) {
            log.error("Error processing audit message: " + e.getMessage());
            if ((e instanceof RuntimeException) || (e instanceof java.rmi.RemoteException)) {
                // all unchecked exceptions or java.rmi.RemoteExceptions will result in a rollback;
            } else {
                // Checked exceptions that are not RemoteExceptions in a container managed transaction do not cause a rollback.
                // See http://java.sun.com/j2ee/tutorial/1_3_fcs/doc/BMP6.html
                log.warn("Forcing rollback, exception was of type " + e.getClass());
            }
            throw new EJBException(e);
        }
    }
    
    abstract protected Auditor getAuditor();
}
