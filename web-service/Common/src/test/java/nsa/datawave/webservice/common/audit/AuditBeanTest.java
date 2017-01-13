package nsa.datawave.webservice.common.audit;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.ws.rs.core.MultivaluedMap;

import nsa.datawave.webservice.common.audit.Auditor.AuditType;
import nsa.datawave.webservice.common.exception.DatawaveWebApplicationException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.TopicConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.TopicConfigurationImpl;
import org.hornetq.jms.server.embedded.EmbeddedJMS;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;

public class AuditBeanTest {
    
    private String userDN = "CN=Last First Middle sid, OU=acme";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";
    Set<String> authorizationsSet = new LinkedHashSet<>();
    private AuditType auditType = AuditType.ACTIVE;
    
    private TestAuditBean audit = new TestAuditBean();
    
    @Before
    public void setup() throws JMSException, IllegalArgumentException, IllegalAccessException {
        Logger.getLogger(AuditBean.class).setLevel(Level.OFF);
        authorizationsSet.add("AUTH1");
        authorizationsSet.add("AUTH2");
        Whitebox.setInternalState(audit, "auditParameters", new AuditParameters());
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
    public void testMissingUserDN() throws Exception {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.put(AuditParameters.QUERY_STRING, Collections.singletonList(query));
        p.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList(authorizations));
        p.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(auditType.name()));
        p.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        audit.audit(p);
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
    public void testMissingQuery() throws Exception {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.put(AuditParameters.USER_DN, Collections.singletonList(userDN));
        p.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList(authorizations));
        p.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(auditType.name()));
        p.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        audit.audit(p);
    }
    
    @Test(expected = DatawaveWebApplicationException.class)
    public void testMissingAuthorizations() throws Exception {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.put(AuditParameters.USER_DN, Collections.singletonList(userDN));
        p.put(AuditParameters.QUERY_STRING, Collections.singletonList(query));
        p.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(auditType.name()));
        p.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        audit.audit(p);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testAudit() throws Exception {
        char[] q = new char[6 * 1024 * 1024];
        for (int i = 0; i < (6 * 1024 * 1024); i++) {
            q[i] = 'a';
        }
        String query = new String(q);
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        String serverDN = "CN=servername, OU=IAMNOTAPERSON";
        p.put(AuditParameters.USER_DN, Collections.singletonList(serverDN));
        p.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList(authorizations));
        p.put(AuditParameters.QUERY_STRING, Collections.singletonList(query));
        p.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(auditType.name()));
        p.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        
        Configuration configuration = new ConfigurationImpl();
        configuration.setPersistenceEnabled(false);
        configuration.setSecurityEnabled(false);
        configuration.setClusterUser("test");
        configuration.setClusterPassword("test");
        configuration.setJournalDirectory(System.getProperty("java.io.tmpdir"));
        Set<Role> roles = new HashSet<>();
        roles.add(new Role("test", true, true, true, true, true, true, true));
        configuration.getSecurityRoles().put("test", roles);
        configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
        
        TransportConfiguration connectorConfiguration = new TransportConfiguration(InVMConnectorFactory.class.getName());
        configuration.getConnectorConfigurations().put("connector", connectorConfiguration);
        
        JMSConfiguration jmsConfiguration = new JMSConfigurationImpl();
        
        ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl("cf", false, Lists.newArrayList("connector"), "/cf");
        assertTrue("Expected test message length (" + q.length + ") to be greater than min large message size (" + cfConfig.getMinLargeMessageSize() + ").",
                        q.length > cfConfig.getMinLargeMessageSize());
        jmsConfiguration.getConnectionFactoryConfigurations().add(cfConfig);
        
        TopicConfiguration auditTopicConfiguration = new TopicConfigurationImpl("AuditTopic", "topic/audit");
        TopicConfiguration auditLogTopicConfiguration = new TopicConfigurationImpl("LogAuditTopic", "topic/Audit.Log");
        TopicConfiguration auditAccumuloTopicConfiguration = new TopicConfigurationImpl("AccumuloAuditTopic", "topic/Audit.Accumulo");
        TopicConfiguration auditRemoteTopicConfiguration = new TopicConfigurationImpl("RemoteAuditTopic", "topic/Audit.Remote");
        jmsConfiguration.getTopicConfigurations().add(auditTopicConfiguration);
        jmsConfiguration.getTopicConfigurations().add(auditLogTopicConfiguration);
        jmsConfiguration.getTopicConfigurations().add(auditAccumuloTopicConfiguration);
        jmsConfiguration.getTopicConfigurations().add(auditRemoteTopicConfiguration);
        
        EmbeddedJMS jmsServer = new EmbeddedJMS();
        jmsServer.setConfiguration(configuration);
        jmsServer.setJmsConfiguration(jmsConfiguration);
        jmsServer.start();
        
        ConnectionFactory cf = (ConnectionFactory) jmsServer.lookup("/cf");
        try (JMSContext context = cf.createContext("test", "test");
                        JMSConsumer logConsumer = context.createConsumer(context.createTopic("LogAuditTopic"));
                        JMSConsumer accumuloConsumer = context.createConsumer(context.createTopic("AccumuloAuditTopic"));
                        JMSConsumer remoteConsumer = context.createConsumer(context.createTopic("RemoteAuditTopic"))) {
            
            Whitebox.setInternalState(audit, JMSContext.class, context);
            authorizationsSet.add("AUTH1");
            authorizationsSet.add("AUTH2");
            
            PowerMock.resetAll();
            List<String> auditTopics = new ArrayList<>();
            auditTopics.add("AuditTopic");
            auditTopics.add("LogAuditTopic");
            auditTopics.add("AccumuloAuditTopic");
            auditTopics.add("RemoteAuditTopic");
            Whitebox.setInternalState(audit, "auditTopics", auditTopics);
            PowerMock.replayAll();
            audit.audit(p);
            PowerMock.verifyAll();
            
            Message messageReceived;
            AuditParameters original = new AuditParameters();
            original.validate(p);
            
            // Check Log AuditTopic, should receive message
            messageReceived = logConsumer.receive(1000);
            AuditParameters received = original.fromMap(messageReceived.getBody(Map.class));
            original.setQueryDate(received.getQueryDate());
            Assert.assertNotNull(messageReceived);
            Assert.assertEquals(original, received);
            
            // Check Accumulo AuditTopic, should receive message
            messageReceived = accumuloConsumer.receive(1000);
            received = original.fromMap(messageReceived.getBody(Map.class));
            original.setQueryDate(received.getQueryDate());
            Assert.assertNotNull(messageReceived);
            Assert.assertEquals(original, received);
            
            // Check Remote AuditTopic, should receive message
            messageReceived = remoteConsumer.receive(1000);
            received = original.fromMap(messageReceived.getBody(Map.class));
            original.setQueryDate(received.getQueryDate());
            Assert.assertNotNull(messageReceived);
            Assert.assertEquals(original, received);
        } finally {
            jmsServer.stop();
        }
    }
    
    public static class TestAuditBean extends AuditBean {
        public boolean sendAudit = true;
        public AuditParameters msg;
        
        @Override
        protected void sendMessage(AuditParameters parameters) throws Exception {
            this.msg = parameters;
            if (sendAudit)
                super.sendMessage(parameters);
        }
    }
}
