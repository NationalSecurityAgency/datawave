package datawave.core.common.logging;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadConfigurableLogger {

    private final Logger log;
    private static final ThreadLocal<Map<String,Level>> logToLevelMap = ThreadLocal.withInitial(HashMap::new);

    private ThreadConfigurableLogger(Logger log) {
        this.log = log;
    }

    public static ThreadConfigurableLogger getLogger(final Class<?> clazz) {
        return new ThreadConfigurableLogger(LogManager.getLogger(clazz));
    }

    public static ThreadConfigurableLogger getLogger(final String name) {
        return new ThreadConfigurableLogger(LogManager.getLogger(name));
    }

    public void debug(Object message) {
        if (shouldLog(Level.DEBUG)) {
            log.debug(message);
        }
    }

    public void debug(Object message, Throwable t) {
        if (shouldLog(Level.DEBUG)) {
            log.debug(message, t);
        }
    }

    public void error(Object message) {
        if (shouldLog(Level.ERROR)) {
            log.error(message);
        }
    }

    public void error(Object message, Throwable t) {
        if (shouldLog(Level.ERROR)) {
            log.error(message, t);
        }
    }

    public void fatal(Object message) {
        if (shouldLog(Level.FATAL)) {
            log.fatal(message);
        }
    }

    public void fatal(Object message, Throwable t) {
        if (shouldLog(Level.FATAL)) {
            log.fatal(message, t);
        }
    }

    public void info(Object message) {
        if (shouldLog(Level.INFO)) {
            log.info(message);
        }
    }

    public void info(Object message, Throwable t) {
        if (shouldLog(Level.INFO)) {
            log.info(message, t);
        }
    }

    public void log(Level level, Object message) {
        if (shouldLog(level)) {
            log.log(level, message);
        }
    }

    public void log(Level level, Object message, Throwable t) {
        if (shouldLog(level)) {
            log.log(level, message, t);
        }
    }

    public void trace(Object message) {
        if (shouldLog(Level.TRACE)) {
            log.trace(message);
        }
    }

    public void trace(Object message, Throwable t) {
        if (shouldLog(Level.TRACE)) {
            log.trace(message, t);
        }
    }

    public void warn(Object message) {
        if (shouldLog(Level.WARN)) {
            log.warn(message);
        }
    }

    public void warn(Object message, Throwable t) {
        if (shouldLog(Level.WARN)) {
            log.warn(message, t);
        }
    }

    private boolean shouldLog(Level requestedLevel) {
        Level allowedLevel = getLevelForThread();
        return allowedLevel == null || requestedLevel.isMoreSpecificThan(allowedLevel);
    }

    public Level getLevelForThread() {
        Map<String,Level> levelMap = logToLevelMap.get();
        return (levelMap != null) ? levelMap.get(log.getName()) : null;
    }

    public void setLevelForThread(Level level) {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.put(log.getName(), level);
        }
    }

    public void clearLevelForThread() {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.remove(log.getName());
        }
    }

    public static void setLevelForThread(String name, Level level) {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.put(name, level);
        }
    }

    public static Level getLevelForThread(String name) {
        Map<String,Level> levelMap = logToLevelMap.get();
        return (levelMap != null) ? levelMap.get(name) : null;
    }

    public static void clearThreadLevels() {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.clear();
        }
    }

    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }
}
