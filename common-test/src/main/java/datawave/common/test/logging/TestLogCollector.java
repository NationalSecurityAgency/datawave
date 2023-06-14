package datawave.common.test.logging;

import org.apache.log4j.bridge.FilterAdapter;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.junit.rules.ExternalResource;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * JUnit Rule for aggregating the log output from a given set of loggers into a simple string list.
 * <p>
 * This class replaces our legacy CommonTestAppender, which implemented a pattern of usage that is broken and no longer valid in log4j 2x
 * </p>
 */
public class TestLogCollector extends ExternalResource {

    private List<String> messages = new ArrayList<>();

    private List<LogAppender> loggers;
    private Writer writer = new CharArrayWriter();

    private TestLogCollector(Builder b) {
        this.loggers = b.classLoggers;
    }

    @Override
    protected void before() {
        StringLayout layout = PatternLayout.newBuilder().withPattern(PatternLayout.DEFAULT_CONVERSION_PATTERN).build();
        Appender appender = WriterAppender.newBuilder().setName(TestLogCollector.class.getSimpleName()).setFilter(new FilterAdapter(new Filter() {
            @Override
            public int decide(LoggingEvent event) {
                messages.add(event.getMessage().toString());
                return Filter.ACCEPT;
            }
        })).setLayout(layout).setTarget(writer).build();
        appender.start();
        this.loggers.stream().forEach(l -> l.setAppender(appender));
    }

    @Override
    protected void after() {
        this.loggers.stream().forEach(l -> l.reset());
        clearMessages();
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
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

        public Builder with(Class<?> clazz, org.apache.log4j.Level level) {
            classLoggers.add(new LogAppender(clazz, Level.valueOf(level.toString())));
            return this;
        }

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
        Class<?> clazz;
        Logger logger;
        Appender appender;
        Level oldLevel;

        LogAppender(Class<?> clazz, Level level) {
            this.logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(clazz);
            this.oldLevel = logger.getLevel();
            this.clazz = clazz;
            Configurator.setLevel(clazz.getCanonicalName(), level);
        }

        void setAppender(Appender appender) {
            this.appender = appender;
            this.logger.addAppender(this.appender);
        }

        void reset() {
            this.logger.removeAppender(this.appender);
            Configurator.setLevel(clazz.getCanonicalName(), oldLevel);
        }
    }
}
