package datawave.common.test.logging;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.bridge.FilterAdapter;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestLogCollectorJUnit5 implements BeforeEachCallback, AfterEachCallback {

    private final List<String> messages = Collections.synchronizedList(new ArrayList<>());
    private final List<LogAppender> loggers;
    private final Writer writer = new CharArrayWriter();

    private TestLogCollectorJUnit5(Builder builder) {
        this.loggers = builder.classLoggers;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        StringLayout layout = PatternLayout.newBuilder().setPattern(PatternLayout.DEFAULT_CONVERSION_PATTERN).build();
        Appender appender = WriterAppender.newBuilder().setName(TestLogCollectorJUnit5.class.getSimpleName()).setFilter(FilterAdapter.adapt(new Filter() {
            @Override
            public int decide(LoggingEvent event) {
                messages.add(event.getMessage().toString());
                return Filter.ACCEPT;
            }
        })).setLayout(layout).setTarget(writer).build();
        appender.start();
        this.loggers.forEach(logger -> logger.setAppender(appender));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        this.loggers.forEach(LogAppender::reset);
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
        private final ArrayList<LogAppender> classLoggers = new ArrayList<>();

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

        public TestLogCollectorJUnit5 build() {
            return new TestLogCollectorJUnit5(this);
        }
    }

    private static class LogAppender {
        final Class<?> clazz;
        final Logger logger;
        Appender appender;
        final Level oldLevel;

        LogAppender(Class<?> clazz, Level level) {
            this.logger = (Logger) LogManager.getLogger(clazz);
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
