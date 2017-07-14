package datawave.webservice.common.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Enumeration;
import java.util.ResourceBundle;

public class ThreadConfigurableLogger extends Logger {
    
    public static ThreadConfigurableLogger getLogger(Class clazz) {
        return new ThreadConfigurableLogger(Logger.getLogger(clazz));
    }
    
    public static ThreadConfigurableLogger getLogger(String name) {
        return new ThreadConfigurableLogger(Logger.getLogger(name));
    }
    
    public static ThreadConfigurableLogger getRootLogger() {
        return new ThreadConfigurableLogger(Logger.getRootLogger());
    }
    
    Logger log = null;
    
    public ThreadConfigurableLogger(Logger log) {
        super(log.getName());
        this.log = log;
    }
    
    @Override
    public synchronized void addAppender(Appender newAppender) {
        log.addAppender(newAppender);
    }
    
    @Override
    public void assertLog(boolean assertion, String msg) {
        log.assertLog(assertion, msg);
    }
    
    @Override
    public void callAppenders(LoggingEvent arg0) {
        log.callAppenders(arg0);
    }
    
    @Override
    public void debug(Object message) {
        if (shouldLog(Level.DEBUG)) {
            this.log.debug(message);
        }
    }
    
    @Override
    public void debug(Object message, Throwable t) {
        if (shouldLog(Level.DEBUG)) {
            this.log.debug(message, t);
        }
    }
    
    @Override
    public void error(Object message) {
        if (shouldLog(Level.ERROR)) {
            this.log.error(message);
        }
    }
    
    @Override
    public void error(Object message, Throwable t) {
        if (shouldLog(Level.ERROR)) {
            this.log.error(message, t);
        }
    }
    
    @Override
    public void fatal(Object message) {
        if (shouldLog(Level.FATAL)) {
            this.log.fatal(message);
        }
    }
    
    @Override
    public void fatal(Object message, Throwable t) {
        if (shouldLog(Level.FATAL)) {
            this.log.fatal(message, t);
        }
    }
    
    @Override
    public boolean getAdditivity() {
        return log.getAdditivity();
    }
    
    @Override
    public synchronized Enumeration getAllAppenders() {
        return log.getAllAppenders();
    }
    
    @Override
    public synchronized Appender getAppender(String name) {
        return log.getAppender(name);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public Priority getChainedPriority() {
        return log.getChainedPriority();
    }
    
    @Override
    public Level getEffectiveLevel() {
        return log.getEffectiveLevel();
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public LoggerRepository getHierarchy() {
        return log.getHierarchy();
    }
    
    @Override
    public LoggerRepository getLoggerRepository() {
        return log.getLoggerRepository();
    }
    
    @Override
    public ResourceBundle getResourceBundle() {
        return log.getResourceBundle();
    }
    
    @Override
    public void info(Object message) {
        if (shouldLog(Level.INFO)) {
            this.log.info(message);
        }
    }
    
    @Override
    public void info(Object message, Throwable t) {
        if (shouldLog(Level.INFO)) {
            this.log.info(message, t);
        }
    }
    
    @Override
    public boolean isAttached(Appender appender) {
        return log.isAttached(appender);
    }
    
    @Override
    public boolean isDebugEnabled() {
        return this.log.isDebugEnabled();
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
        return this.log.isTraceEnabled();
    }
    
    @Override
    public void l7dlog(Priority arg0, String arg1, Object[] arg2, Throwable arg3) {
        log.l7dlog(arg0, arg1, arg2, arg3);
    }
    
    @Override
    public void l7dlog(Priority arg0, String arg1, Throwable arg2) {
        log.l7dlog(arg0, arg1, arg2);
    }
    
    @Override
    public void log(Priority priority, Object message) {
        log.log(priority, message);
    }
    
    @Override
    public void log(Priority priority, Object message, Throwable t) {
        log.log(priority, message, t);
    }
    
    @Override
    public void log(String callerFQCN, Priority level, Object message, Throwable t) {
        log.log(callerFQCN, level, message, t);
    }
    
    @Override
    public synchronized void removeAllAppenders() {
        log.removeAllAppenders();
    }
    
    @Override
    public synchronized void removeAppender(Appender appender) {
        log.removeAppender(appender);
    }
    
    @Override
    public synchronized void removeAppender(String name) {
        log.removeAppender(name);
    }
    
    @Override
    public void setAdditivity(boolean additive) {
        log.setAdditivity(additive);
    }
    
    @Override
    public void setLevel(Level level) {
        log.setLevel(level);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public void setPriority(Priority priority) {
        log.setPriority(priority);
    }
    
    @Override
    public void setResourceBundle(ResourceBundle bundle) {
        log.setResourceBundle(bundle);
    }
    
    private boolean shouldLog(Level requestedLevel) {
        
        Level allowedLevel = ThreadLocalLogLevel.getLevel(log.getName());
        if (allowedLevel == null || requestedLevel.isGreaterOrEqual(allowedLevel)) {
            return true;
        } else {
            return false;
        }
        
    }
    
    @Override
    public void trace(Object message) {
        if (shouldLog(Level.TRACE)) {
            this.log.trace(message);
        }
    }
    
    @Override
    public void trace(Object message, Throwable t) {
        if (shouldLog(Level.TRACE)) {
            this.log.trace(message, t);
        }
    }
    
    @Override
    public void warn(Object message) {
        if (shouldLog(Level.WARN)) {
            this.log.warn(message);
        }
    }
    
    @Override
    public void warn(Object message, Throwable t) {
        if (shouldLog(Level.WARN)) {
            this.log.warn(message, t);
        }
    }
}
