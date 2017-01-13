package nsa.datawave.webservice.common.audit;

import java.util.Enumeration;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBException;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Topic;

import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.apache.log4j.Logger;

@RunAs("InternalUser")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.WRITE)
// by default all methods are non-blocking
@MBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class AuditDLQBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private JMSContext context;
    
    @JmxManaged
    public String moveMessageBackToAuditTopic(String dlqQueueName, String auditTopic, String messageId) {
        Queue dlq = context.createQueue(dlqQueueName);
        Topic auditopic = context.createTopic(auditTopic);
        
        try (JMSConsumer consumer = context.createConsumer(dlq, "JMSMessageID = '" + messageId + "'")) {
            Message message = consumer.receiveNoWait();
            if (message != null) {
                context.createProducer().send(auditopic, message);
                return "Moved back message with id " + messageId + " to " + auditTopic;
            } else {
                throw new EJBException("Could not find message with ID " + messageId + " on queue " + dlqQueueName);
            }
        }
    }
    
    @JmxManaged
    public String moveAllMessagesBackToAuditTopic(String dlqQueueName, String auditTopic) {
        log.info("Moving all messages back to " + auditTopic);
        Queue dlq = context.createQueue(dlqQueueName);
        Topic auditopic = context.createTopic(auditTopic);
        StringBuilder sb = new StringBuilder();
        try (JMSConsumer consumer = context.createConsumer(dlq)) {
            Message message;
            JMSProducer producer = context.createProducer();
            while ((message = consumer.receiveNoWait()) != null) {
                producer.send(auditopic, message);
                sb.append("Sent message ").append(message.getJMSMessageID()).append(" back to ").append(auditTopic).append('\n');
            }
        } catch (JMSException e) {
            log.error("Error sending messages back to audit topic.");
        }
        return sb.toString();
        
    }
    
    @JmxManaged
    public String listMessagesOnAuditDLQ(String dlqQueueName) {
        StringBuilder sb = new StringBuilder();
        Queue dlq = context.createQueue(dlqQueueName);
        try (QueueBrowser browser = context.createBrowser(dlq)) {
            log.info("Listing messages in " + dlq.getQueueName());
            for (Enumeration<Message> enumeration = browser.getEnumeration(); enumeration.hasMoreElements();) {
                Message message = enumeration.nextElement();
                sb.append(message.toString()); // TODO:WILDFLY - list this message as json
            }
        } catch (JMSException e) {
            log.error("Problem listing messages", e);
        }
        return sb.toString();
    }
    
}
