package datawave.common.test.logging;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Appender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.QuietWriter;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.LoggingEvent;

public class CommonTestAppender extends WriterAppender {
    
    private static final class DefaultErrorHandler implements ErrorHandler {
        private final Appender appender;
        
        public DefaultErrorHandler(final Appender appender) {
            this.appender = appender;
        }
        
        @Override
        /** @since 1.2 */
        public void setLogger(Logger logger) {
            // Deprecated
        }
        
        public void error(String message, Exception ioe, int errorCode) {
            appender.close();
            LogLog.error("IO failure for appender named " + appender.getName(), ioe);
        }
        
        @Override
        public void error(String message) {
            // Deprecated
        }
        
        @Override
        /** @since 1.2 */
        public void error(String message, Exception e, int errorCode, LoggingEvent event) {
            // Deprecated
        }
        
        @Override
        /** @since 1.2 */
        public void setAppender(Appender appender) {
            // Deprecated
        }
        
        @Override
        /** @since 1.2 */
        public void setBackupAppender(Appender appender) {
            // Deprecated
        }
        
        @Override
        public void activateOptions() {
            // Deprecated
        }
        
    }
    
    protected ByteArrayOutputStream baos = null;
    
    public List<String> retrieveLogsEntries() {
        
        List<String> uutLogEntries = new ArrayList<>();
        try {
            
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            BufferedReader br = new BufferedReader(new InputStreamReader(bais));
            
            String line = null;
            
            while (null != (line = br.readLine())) {
                
                uutLogEntries.add(line);
            }
            
        } catch (Exception e) {
            
            // Ignore any issues (technically there shouldn't be any but
            // if they are we've got bigger issues that worrying about
            // determining if a unit under test behaved as excepted...
        }
        
        return uutLogEntries;
    }
    
    public void resetLogs() {
        
        baos = new ByteArrayOutputStream();
        
        Writer writer = new OutputStreamWriter(baos);
        
        ErrorHandler errorHandler = new CommonTestAppender.DefaultErrorHandler(this);
        
        this.qw = new QuietWriter(writer, errorHandler);
    }
    
    public CommonTestAppender() {
        
        this.resetLogs();
        
        this.setName("CommonTestAppender");
        this.setLayout(new EnhancedPatternLayout(EnhancedPatternLayout.TTCC_CONVERSION_PATTERN));
        this.setImmediateFlush(true);
    }
    
}
