package datawave.microservice.annotation.writers.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.test.v1.AnnotationTestDataUtil;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.microservice.annotation.writers.AnnotationWriter;
import lombok.Getter;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = {LogAnnotationWriterTest.LogAnnotationWriterTestConfiguration.class})
@ActiveProfiles({"LogAnnotationWriterTest", "log-enabled"})
public class LogAnnotationWriterTest {

    @Autowired
    private AnnotationWriter logAnnotationWriter;

    @Autowired
    private ApplicationContext context;

    @Test
    public void testBeansPresent() {
        assertTrue(context.containsBean("logAnnotationSink"), "expected logAnnotationSink to be present");
        assertTrue(context.containsBean("logAnnotationWriter"), "expected logAnnotationWriter to be present");
    }

    @Test
    public void testAnnotationWrite() throws Exception {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

        AbstractConfiguration config = (AbstractConfiguration) ctx.getConfiguration();

        TestAppender testAppender = new TestAppender();
        testAppender.start();
        config.addAppender(testAppender);
        AppenderRef[] refs = new AppenderRef[] {AppenderRef.createAppenderRef(testAppender.getName(), null, null)};

        final String loggerName = "datawave.microservice.annotation.writers.log";

        LoggerConfig loggerConfig = LoggerConfig.newBuilder().withAdditivity(true).withLevel(Level.ALL).withLoggerName(loggerName).withIncludeLocation("true")
                        .withRefs(refs).withConfig(config).build();
        loggerConfig.addAppender(testAppender, null, null);

        config.addLogger(loggerName, loggerConfig);
        ctx.updateLoggers();

        testAppender.clear();

        // write the annotation and source.
        Annotation partialAnnotation = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation testAnnotation = AnnotationUtils.injectAllHashes(partialAnnotation);
        Optional<Annotation> result = logAnnotationWriter.write(testAnnotation);
        assertTrue(result.isPresent());

        Annotation writtenAnnotation = result.get();
        final String writtenAnnotationJson = AnnotationJsonUtils.annotationToJsonWithIds(writtenAnnotation) + "\n";

        assertEquals(1, testAppender.getLog().size(), "expected testAppender to contain a log message");
        LogEvent logEvent = testAppender.getLog().get(0);
        assertEquals(writtenAnnotationJson, logEvent.getMessage().getFormattedMessage(), "expected testAppender to contain the correct log message");
        assertEquals(Level.INFO, logEvent.getLevel(), "expected the log level to be INFO");
    }

    @Configuration
    @Profile("LogAnnotationWriterTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class LogAnnotationWriterTestConfiguration {}

    @Getter
    static class TestAppender extends AbstractAppender {
        private final List<LogEvent> log = new ArrayList<>();

        protected TestAppender() {
            super("TestAppender", null, null, false, null);
        }

        @Override
        public void append(LogEvent event) {
            log.add(event);
        }

        public void clear() {
            log.clear();
        }
    }
}
