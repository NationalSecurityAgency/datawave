package datawave.microservice.audit.replay.status;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the status of an audit replay. File status is listed on a per-file basis.
 */
public class Status implements Serializable {
    private static final long serialVersionUID = 5090967818014506871L;
    
    public enum ReplayState {
        CREATED, RUNNING, STOPPED, FINISHED, FAILED
    }
    
    public enum FileState {
        QUEUED, RUNNING, FINISHED, FAILED
    }
    
    private String id;
    private ReplayState state;
    private String pathUri;
    private long sendRate;
    private List<FileStatus> files = new ArrayList<>();
    private Date lastUpdated;
    private boolean replayUnfinishedFiles;
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public ReplayState getState() {
        return state;
    }
    
    public void setState(ReplayState state) {
        this.state = state;
    }
    
    public String getPathUri() {
        return pathUri;
    }
    
    public void setPathUri(String pathUri) {
        this.pathUri = pathUri;
    }
    
    public long getSendRate() {
        return sendRate;
    }
    
    public void setSendRate(long sendRate) {
        this.sendRate = sendRate;
    }
    
    public List<FileStatus> getFiles() {
        return files;
    }
    
    public void setFiles(List<FileStatus> files) {
        this.files = files;
    }
    
    public Date getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public boolean isReplayUnfinishedFiles() {
        return replayUnfinishedFiles;
    }
    
    public void setReplayUnfinishedFiles(boolean replayUnfinishedFiles) {
        this.replayUnfinishedFiles = replayUnfinishedFiles;
    }
    
    @Override
    public String toString() {
        return "{id:" + id + ", state:" + state + ", pathUri:" + pathUri + ", sendRate:" + sendRate + ", files:["
                        + String.join(",", files.stream().map(FileStatus::toString).collect(Collectors.toSet())) + "], lastUpdated:" + lastUpdated
                        + ", replayUnfinishedFiles:" + replayUnfinishedFiles + "}";
    }
    
    public static final class FileStatus implements Serializable {
        private static final long serialVersionUID = 3057248386635077592L;
        
        private FileState state;
        private String pathUri;
        private long linesRead;
        private long auditsSent;
        private long auditsFailed;
        private long parseFailures;
        private boolean encounteredError;
        
        public FileStatus() {
            
        }
        
        public FileStatus(String pathUri, FileState state) {
            this.pathUri = pathUri;
            this.state = state;
            this.linesRead = 0;
            this.auditsSent = 0;
            this.auditsFailed = 0;
            this.parseFailures = 0;
            this.encounteredError = false;
        }
        
        public FileState getState() {
            return state;
        }
        
        public void setState(FileState state) {
            this.state = state;
        }
        
        public String getPathUri() {
            return pathUri;
        }
        
        public void setPathUri(String pathUri) {
            this.pathUri = pathUri;
        }
        
        public long getLinesRead() {
            return linesRead;
        }
        
        public void setLinesRead(long linesRead) {
            this.linesRead = linesRead;
        }
        
        public long getAuditsSent() {
            return auditsSent;
        }
        
        public void setAuditsSent(long auditsSent) {
            this.auditsSent = auditsSent;
        }
        
        public long getAuditsFailed() {
            return auditsFailed;
        }
        
        public void setAuditsFailed(long auditsFailed) {
            this.auditsFailed = auditsFailed;
        }
        
        public long getParseFailures() {
            return parseFailures;
        }
        
        public void setParseFailures(long parseFailures) {
            this.parseFailures = parseFailures;
        }
        
        public boolean isEncounteredError() {
            return encounteredError;
        }
        
        public void setEncounteredError(boolean encounteredError) {
            this.encounteredError = encounteredError;
        }
        
        @Override
        public String toString() {
            return "{pathUri:" + pathUri + ", state:" + state + ", linesRead:" + linesRead + ", auditsSent:" + auditsSent + ", auditsFailed" + auditsFailed
                            + ", encounteredError:" + encounteredError + "}";
        }
    }
}
