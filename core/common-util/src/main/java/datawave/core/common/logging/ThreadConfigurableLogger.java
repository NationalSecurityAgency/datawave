package datawave.core.common.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.util.HashMap;
import java.util.Map;

public class ThreadConfigurableLogger extends Logger {

    private final Logger log;
    private static ThreadLocal<Map<String,Level>> logToLevelMap = ThreadLocal.withInitial(HashMap::new);

    public static ThreadConfigurableLogger getLogger(Class clazz) {
        return new ThreadConfigurableLogger(Logger.getLogger(clazz));
    }

    public static ThreadConfigurableLogger getLogger(String name) {
        return new ThreadConfigurableLogger(Logger.getLogger(name));
    }

    public static ThreadConfigurableLogger getRootLogger() {
        return new ThreadConfigurableLogger(Logger.getRootLogger());
    }

    public ThreadConfigurableLogger(Logger log) {
        super(log.getName());
        this.log = log;
    }

    @Override
    public void debug(Object message) {
        if (shouldLog(Level.DEBUG)) {
            log.debug(message);
        }
    }

    @Override
    public void debug(Object message, Throwable t) {
        if (shouldLog(Level.DEBUG)) {
            log.debug(message, t);
        }
    }

    @Override
    public void error(Object message) {
        if (shouldLog(Level.ERROR)) {
            log.error(message);
        }
    }

    @Override
    public void error(Object message, Throwable t) {
        if (shouldLog(Level.ERROR)) {
            log.error(message, t);
        }
    }

    @Override
    public void fatal(Object message) {
        if (shouldLog(Level.FATAL)) {
            log.fatal(message);
        }
    }

    @Override
    public void fatal(Object message, Throwable t) {
        if (shouldLog(Level.FATAL)) {
            log.fatal(message, t);
        }
    }

    @Override
    public void info(Object message) {
        if (shouldLog(Level.INFO)) {
            log.info(message);
        }
    }

    @Override
    public void info(Object message, Throwable t) {
        if (shouldLog(Level.INFO)) {
            log.info(message, t);
        }
    }

    @Override
    public void l7dlog(Priority arg0, String arg1, Object[] arg2, Throwable arg3) {
        if (shouldLog(Level.toLevel(arg0.toInt()))) {
            log.l7dlog(arg0, arg1, arg2, arg3);
        }
    }

    @Override
    public void l7dlog(Priority arg0, String arg1, Throwable arg2) {
        if (shouldLog(Level.toLevel(arg0.toInt()))) {
            log.l7dlog(arg0, arg1, arg2);
        }
    }

    @Override
    public void log(Priority priority, Object message) {
        if (shouldLog(Level.toLevel(priority.toInt()))) {
            log.log(priority, message);
        }
    }

    @Override
    public void log(Priority priority, Object message, Throwable t) {
        if (shouldLog(Level.toLevel(priority.toInt()))) {
            log.log(priority, message, t);
        }
    }

    @Override
    public void log(String callerFQCN, Priority level, Object message, Throwable t) {
        if (shouldLog(Level.toLevel(level.toInt()))) {
            log.log(callerFQCN, level, message, t);
        }
    }

    private boolean shouldLog(Level requestedLevel) {
        Level allowedLevel = getLevelForThread();
        if (allowedLevel == null || requestedLevel.isGreaterOrEqual(allowedLevel)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void trace(Object message) {
        if (shouldLog(Level.TRACE)) {
            log.trace(message);
        }
    }

    @Override
    public void trace(Object message, Throwable t) {
        if (shouldLog(Level.TRACE)) {
            log.trace(message, t);
        }
    }

    @Override
    public void warn(Object message) {
        if (shouldLog(Level.WARN)) {
            log.warn(message);
        }
    }

    @Override
    public void warn(Object message, Throwable t) {
        if (shouldLog(Level.WARN)) {
            log.warn(message, t);
        }
    }

    @Override
    public synchronized void addAppender(Appender newAppender) {
        log.addAppender(newAppender);
    }

    @Override
    public boolean isAttached(Appender appender) {
        return log.isAttached(appender);
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public boolean isEnabledFor(Priority level) {
        return log.isEnabledFor(level);
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public void setLevel(Level level) {
        log.setLevel(level);
    }

    public Level getLevelForThread() {
        Level level = null;
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            level = levelMap.get(getName());
        }
        return level;
    }

    public void setLevelForThread(Level level) {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.put(getName(), level);
        }
    }

    public void clearLevelForThread() {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.remove(getName());
        }
    }

    public static void setLevelForThread(String name, Level level) {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.put(name, level);
        }
    }

    public static Level getLevelForThread(String name) {
        Level level = null;
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            level = levelMap.get(name);
        }
        return level;
    }

    public static void clearThreadLevels() {
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.clear();
        }
    }
}
