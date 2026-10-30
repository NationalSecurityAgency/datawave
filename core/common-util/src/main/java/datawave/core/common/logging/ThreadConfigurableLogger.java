package datawave.core.common.logging;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.AbstractLogger;
import org.slf4j.spi.CallerBoundaryAware;
import org.slf4j.spi.LocationAwareLogger;
import org.slf4j.spi.LoggingEventAware;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * An SLF4J logger that can further restrict the backend's log level for the current thread. Overrides are shared by logger name, and must be cleared before
 * returning a thread to a pool.
 */
public class ThreadConfigurableLogger extends AbstractLogger implements LoggingEventAware {

    private static final long serialVersionUID = 1L;
    private static final int OFF = Integer.MAX_VALUE;
    private static final ThreadLocal<Map<String,Integer>> logToLevelMap = ThreadLocal.withInitial(HashMap::new);

    private final Logger log;

    public static Logger getLogger(final Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    public static Logger getLogger(final String name) {
        return new ThreadConfigurableLogger(LoggerFactory.getLogger(name));
    }

    public static Logger getRootLogger() {
        return getLogger(Logger.ROOT_LOGGER_NAME);
    }

    public ThreadConfigurableLogger(Logger log) {
        this.log = log;
        this.name = log.getName();
    }

    private boolean shouldLog(Level requestedLevel) {
        Integer allowedLevel = logToLevelMap.get().get(getName());
        return allowedLevel == null || requestedLevel.toInt() >= allowedLevel;
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return AbstractLogger.class.getName();
    }

    @Override
    protected void handleNormalizedLoggingCall(Level level, Marker marker, String message, Object[] arguments, Throwable cause) {
        if (log instanceof LocationAwareLogger) {
            ((LocationAwareLogger) log).log(marker, getFullyQualifiedCallerName(), level.toInt(), message, arguments, cause);
        } else {
            LoggingEventBuilder builder = log.atLevel(level).setCause(cause);
            if (marker != null) {
                builder.addMarker(marker);
            }
            if (builder instanceof CallerBoundaryAware) {
                ((CallerBoundaryAware) builder).setCallerBoundary(getFullyQualifiedCallerName());
            }
            if (arguments == null) {
                builder.log(message);
            } else {
                builder.log(message, arguments);
            }
        }
    }

    @Override
    public void log(LoggingEvent event) {
        if (!isEnabledForLevel(event.getLevel())) {
            return;
        }
        LoggingEventBuilder builder = log.atLevel(event.getLevel()).setCause(event.getThrowable());
        if (event.getMarkers() != null) {
            event.getMarkers().forEach(builder::addMarker);
        }
        if (event.getKeyValuePairs() != null) {
            event.getKeyValuePairs().forEach(pair -> builder.addKeyValue(pair.key, pair.value));
        }
        if (builder instanceof CallerBoundaryAware) {
            ((CallerBoundaryAware) builder).setCallerBoundary(event.getCallerBoundary());
        }
        if (event.getArgumentArray() == null) {
            builder.log(event.getMessage());
        } else {
            builder.log(event.getMessage(), event.getArgumentArray());
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return shouldLog(Level.TRACE) && log.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return shouldLog(Level.TRACE) && log.isTraceEnabled(marker);
    }

    @Override
    public boolean isDebugEnabled() {
        return shouldLog(Level.DEBUG) && log.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return shouldLog(Level.DEBUG) && log.isDebugEnabled(marker);
    }

    @Override
    public boolean isInfoEnabled() {
        return shouldLog(Level.INFO) && log.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return shouldLog(Level.INFO) && log.isInfoEnabled(marker);
    }

    @Override
    public boolean isWarnEnabled() {
        return shouldLog(Level.WARN) && log.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return shouldLog(Level.WARN) && log.isWarnEnabled(marker);
    }

    @Override
    public boolean isErrorEnabled() {
        return shouldLog(Level.ERROR) && log.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return shouldLog(Level.ERROR) && log.isErrorEnabled(marker);
    }

    /** Returns the thread's explicit level, or null if no level is set or logging is disabled. */
    public Level getLevelForThread() {
        return getLevelForThread(getName());
    }

    public void setLevelForThread(Level level) {
        setLevelForThread(getName(), level);
    }

    public void clearLevelForThread() {
        logToLevelMap.get().remove(getName());
    }

    public static void setLevelForThread(String name, Level level) {
        if (level == null) {
            logToLevelMap.get().remove(name);
        } else {
            logToLevelMap.get().put(name, level.toInt());
        }
    }

    public static Level getLevelForThread(String name) {
        Integer level = logToLevelMap.get().get(name);
        return level == null || level == OFF ? null : Level.intToLevel(level);
    }

    public static void disableForThread(String name) {
        logToLevelMap.get().put(name, OFF);
    }

    public static void clearThreadLevels() {
        logToLevelMap.remove();
    }
}
