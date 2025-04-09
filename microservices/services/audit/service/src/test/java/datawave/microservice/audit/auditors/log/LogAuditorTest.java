package datawave.microservice.audit.auditors.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = LogAuditorTest.LogAuditorTestConfiguration.class)
@ActiveProfiles({"LogAuditorTest", "log-enabled"})
public class LogAuditorTest {
    
    @Autowired
    private Auditor logAuditor;
    
    @Autowired
    private ApplicationContext context;
    
    private TestAppender testAppender;
    
    @PostConstruct
    public void LogAuditorTestInit() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        AbstractConfiguration config = (AbstractConfiguration) ctx.getConfiguration();
        testAppender = new TestAppender();
        testAppender.start();
        config.addAppender(testAppender);
        AppenderRef[] refs = new AppenderRef[] {AppenderRef.createAppenderRef(testAppender.getName(), null, null)};
        LoggerConfig loggerConfig = LoggerConfig.createLogger(false, Level.ALL, "datawave.microservice.audit.auditors.log", "true", refs, null, config, null);
        loggerConfig.addAppender(testAppender, null, null);
        config.addLogger("datawave.microservice.audit.auditors.log", loggerConfig);
        ctx.updateLoggers();
    }
    
    @Test
    public void testBeansPresent() {
        assertTrue(context.containsBean("logAuditSink"));
        assertTrue(context.containsBean("logAuditor"));
    }
    
    @Test
    public void testActiveAudit() throws Exception {
        testAppender.clear();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(new Date());
        
        logAuditor.audit(auditParams);
        
        assertEquals(1, testAppender.getLog().size());
        LogEvent logEvent = testAppender.getLog().get(0);
        assertEquals(auditParams.toString(), logEvent.getMessage().getFormattedMessage());
        assertEquals(Level.INFO, logEvent.getLevel());
    }
    
    @Test
    public void testNoneAudit() throws Exception {
        testAppender.clear();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.NONE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(new Date());
        
        logAuditor.audit(auditParams);
        
        assertEquals(0, testAppender.getLog().size());
    }
    
    @Configuration
    @Profile("LogAuditorTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class LogAuditorTestConfiguration {}
    
    static class TestAppender extends AbstractAppender {
        private final List<LogEvent> log = new ArrayList<>();
        
        protected TestAppender() {
            super("TestAppender", null, null, false, null);
        }
        
        @Override
        public void append(LogEvent event) {
            log.add(event);
        }
        
        public List<LogEvent> getLog() {
            return log;
        }
        
        public void clear() {
            log.clear();
        }
    }
}
