package datawave.microservice.audit.auditors.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

/**
 * An implementation for {@link Auditor}, which writes JSON formatted audit messages to a file.
 */
public class FileAuditor implements Auditor {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    protected static final ObjectMapper mapper = new ObjectMapper();
    
    protected final SimpleDateFormat sdf;
    protected final ReentrantLock writeLock = new ReentrantLock(true);
    
    protected long maxFileLengthMB;
    protected long maxFileAgeSeconds;
    
    protected FileSystem fileSystem;
    protected Path path;
    
    protected Path currentFile = null;
    protected Date creationDate = null;
    
    protected FileAuditor(Builder<?> builder) throws URISyntaxException, IOException {
        this.maxFileLengthMB = builder.maxFileLengthMB;
        this.maxFileAgeSeconds = builder.maxFileAgeSeconds;
        
        Configuration config = new Configuration();
        
        if (builder.fsConfigResources != null) {
            for (String resource : builder.fsConfigResources) {
                config.addResource(new Path(resource));
            }
        }
        
        path = new Path(builder.path);
        
        if (builder.subPath != null) {
            path = new Path(path, builder.subPath);
        }
        
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(builder.user);
        UserGroupInformation.setLoginUser(ugi);
        
        fileSystem = FileSystem.get(path.toUri(), config);
        
        String sdfString = "yyyyMMdd_HHmmss.SSS'.json'";
        if (builder.prefix != null && !builder.prefix.isEmpty()) {
            sdfString = "'" + builder.prefix + "-'" + sdfString;
        }
        
        this.sdf = new SimpleDateFormat(sdfString);
    }
    
    @Override
    public void audit(AuditParameters auditParameters) throws Exception {
        
        // create the audit path if it doesn't exist
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
        }
        
        // convert the messages to JSON
        String jsonAuditParams = mapper.writeValueAsString(auditParameters.toMap()) + "\n";
        
        writeLock.lock();
        try {
            // if the file/stream is null, doesn't exist, or the file is too old/big, create a new file & output stream
            if (currentFile == null || !fileSystem.exists(currentFile) || isFileTooOld() || isFileTooBig()) {
                createNewFile();
            }
            
            writeAudit(jsonAuditParams);
        } finally {
            writeLock.unlock();
        }
    }
    
    protected void writeAudit(String jsonAuditParams) throws Exception {
        OutputStream appendStream = (fileSystem instanceof LocalFileSystem) ? new FileOutputStream(new File(currentFile.toUri()), true)
                        : fileSystem.append(currentFile);
        appendStream.write(jsonAuditParams.getBytes("UTF8"));
        appendStream.close();
    }
    
    protected void createNewFile() throws IOException, ParseException {
        // create a new file and output stream
        Date currentDate = new Date();
        currentFile = new Path(path, sdf.format(currentDate));
        FSDataOutputStream outStream = fileSystem.create(currentFile);
        outStream.close();
        creationDate = currentDate;
    }
    
    protected boolean isFileTooOld() throws ParseException {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - creationDate.getTime()) >= maxFileAgeSeconds;
    }
    
    protected boolean isFileTooBig() throws IOException {
        return ((double) fileSystem.getFileStatus(currentFile).getLen() / (1024L * 1024L)) >= maxFileLengthMB;
    }
    
    public static class Builder<T extends Builder<T>> {
        protected String user;
        protected String path;
        protected String subPath;
        protected List<String> fsConfigResources;
        protected String prefix;
        protected Long maxFileLengthMB;
        protected Long maxFileAgeSeconds;
        
        public Builder() {
            user = "datawave";
            prefix = "audit";
            maxFileLengthMB = 8192L;
            maxFileAgeSeconds = TimeUnit.HOURS.toSeconds(6);
        }
        
        public String getUser() {
            return user;
        }
        
        public T setUser(String user) {
            if (user != null) {
                this.user = user;
            }
            return (T) this;
        }
        
        public String getPath() {
            return path;
        }
        
        public T setPath(String path) {
            if (path != null) {
                this.path = path;
            }
            return (T) this;
        }
        
        public String getSubPath() {
            return subPath;
        }
        
        public T setSubPath(String subPath) {
            if (subPath != null) {
                this.subPath = subPath;
            }
            return (T) this;
        }
        
        public List<String> getFsConfigResources() {
            return fsConfigResources;
        }
        
        public T setFsConfigResources(List<String> fsConfigResources) {
            this.fsConfigResources = fsConfigResources;
            return (T) this;
        }
        
        public String getPrefix() {
            return prefix;
        }
        
        public T setPrefix(String prefix) {
            if (prefix != null) {
                this.prefix = prefix;
            }
            return (T) this;
        }
        
        public Long getMaxFileLengthMB() {
            return maxFileLengthMB;
        }
        
        public T setMaxFileLengthMB(Long maxFileLengthMB) {
            if (maxFileLengthMB != null) {
                this.maxFileLengthMB = maxFileLengthMB;
            }
            return (T) this;
        }
        
        public Long getMaxFileAgeSeconds() {
            return maxFileAgeSeconds;
        }
        
        public T setMaxFileAgeSeconds(Long maxFileAgeSeconds) {
            if (maxFileAgeSeconds != null) {
                this.maxFileAgeSeconds = maxFileAgeSeconds;
            }
            return (T) this;
        }
        
        public FileAuditor build() throws IOException, URISyntaxException {
            return new FileAuditor(this);
        }
    }
}
