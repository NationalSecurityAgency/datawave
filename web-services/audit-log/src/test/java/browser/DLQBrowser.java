package browser;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class DLQBrowser {
    
    public static void main(String[] args) {
        // to test the DLQ, add the following to the LogAuditBean.java and redeploy
        /*
         * if(am.getJustification().equals("AuditDLQ-unchecked")) { throw new RuntimeException("You should rollback with a unchecked exception"); } else if
         * (am.getJustification().equals("AuditDLQ-checked")) { throw new Exception("You rollback with an checked exception, and see a warning in the log"); }
         */
        // then submit a query with either AuditDLQ-unchecked or AuditDLQ-checked as the justification
        
        // when running this class, you need to add JBOSS_HOME/common/lib/*.jar and JBOSS_HOME/client/*.jar to the classpath
        new DLQBrowser().dumpQueue();
    }
    
    public void dumpQueue() {
        Connection connection = null;
        InitialContext initialContext = null;
        try {
            initialContext = new InitialContext();
            Queue queue = (Queue) initialContext.lookup("/queue/DLQ");
            ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
            connection = cf.createConnection("DATAWAVE", "secret");
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            QueueBrowser browser = session.createBrowser(queue);
            
            Enumeration<?> messageEnum = browser.getEnumeration();
            System.out.println("Current messages in /queue/DLQ");
            while (messageEnum.hasMoreElements()) {
                ObjectMessage message = (ObjectMessage) messageEnum.nextElement();
                System.out.println("- " + message.getObject());
            }
            
            browser.close();
        } catch (NamingException | JMSException e) {
            e.printStackTrace();
        } finally {
            if (initialContext != null) {
                try {
                    initialContext.close();
                } catch (NamingException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
