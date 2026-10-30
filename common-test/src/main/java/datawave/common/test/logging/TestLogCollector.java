package datawave.common.test.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.rules.ExternalResource;

/**
 * JUnit Rule for aggregating the log output from a given set of loggers into a simple string list.
 * <p>
 * This class replaces our legacy CommonTestAppender, which implemented a pattern of usage that is broken and no longer valid in log4j 2x
 * </p>
 *
 * @deprecated Use {@link TestLogCollectorJUnit5} for JUnit 5 tests.
 */
@Deprecated
public class TestLogCollector extends ExternalResource {

    private List<String> messages = Collections.synchronizedList(new ArrayList<>());

    private List<LogAppender> loggers;
    private Appender appender;

    private TestLogCollector(Builder b) {
        this.loggers = b.classLoggers;
    }

    @Override
    protected void before() {
        appender = new AbstractAppender(TestLogCollector.class.getSimpleName(), null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        this.loggers.forEach(logger -> logger.setAppender(appender));
    }

    @Override
    protected void after() {
        this.loggers.stream().forEach(l -> l.reset());
        clearMessages();
        if (appender != null) {
            appender.stop();
            appender = null;
        }
    }

    public List<String> getMessages() {
        return this.messages;
    }

    public void clearMessages() {
        this.messages.clear();
    }

    public static class Builder {

        private ArrayList<LogAppender> classLoggers = new ArrayList<>();

        public Builder with(Class<?> clazz, Level level) {
            classLoggers.add(new LogAppender(clazz, level));
            return this;
        }

        public Builder with(Class<?> clazz, String level) {
            classLoggers.add(new LogAppender(clazz, Level.valueOf(level)));
            return this;
        }

        public TestLogCollector build() {
            return new TestLogCollector(this);
        }
    }

    private static class LogAppender {
        Logger logger;
        Appender appender;
        final Level level;
        Level oldLevel;
        LoggerConfig oldConfig;
        Configuration configuration;

        LogAppender(Class<?> clazz, Level level) {
            LoggerContext context = (LoggerContext) LogManager.getContext(clazz.getClassLoader(), false);
            this.logger = context.getLogger(clazz.getName());
            this.level = level;
        }

        void setAppender(Appender appender) {
            this.configuration = logger.getContext().getConfiguration();
            this.oldConfig = configuration.getLoggers().get(logger.getName());
            if (oldConfig == null) {
                configuration.addLogger(logger.getName(), new LoggerConfig(logger.getName(), level, true));
            } else {
                this.oldLevel = oldConfig.getLevel();
                oldConfig.setLevel(level);
            }
            logger.getContext().updateLoggers();
            this.appender = appender;
            this.logger.addAppender(this.appender);
        }

        void reset() {
            this.logger.removeAppender(this.appender);
            if (oldConfig == null) {
                configuration.removeLogger(logger.getName());
            } else {
                oldConfig.setLevel(oldLevel);
            }
            logger.getContext().updateLoggers();
        }
    }
}
