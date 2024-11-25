package datawave.microservice.audit.replay.runner;

import static java.nio.charset.StandardCharsets.UTF_8;

import static datawave.microservice.audit.replay.status.Status.FileState;
import static datawave.microservice.audit.replay.status.Status.ReplayState;
import static datawave.webservice.common.audit.AuditParameters.AUDIT_ID;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.microservice.audit.replay.config.ReplayProperties;
import datawave.microservice.audit.replay.status.Status;
import datawave.microservice.audit.replay.status.StatusCache;

/**
 * The replay task is responsible for reading the audit messages from disk, parsing them from JSON, and processing them through the audit controller.
 */
public abstract class ReplayTask implements Runnable {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ReplayTask.class);
    
    private final Status status;
    private final StatusCache statusCache;
    private final ReplayProperties replayProperties;
    
    private FileSystem filesystem;
    
    public ReplayTask(Configuration config, Status status, StatusCache statusCache, ReplayProperties replayProperties) throws Exception {
        this.status = status;
        this.statusCache = statusCache;
        this.replayProperties = replayProperties;
        this.filesystem = FileSystem.get(new URI(status.getPathUri()), config);
    }
    
    @Override
    public void run() {
        
        if (status.getState() != ReplayState.RUNNING)
            return;
        
        // if we need to, get a list of files
        if (status.getFiles().isEmpty())
            status.setFiles(listFiles(status.isReplayUnfinishedFiles()));
        
        // sort the files to process. 'RUNNING' first, followed by 'QUEUED'
        List<Status.FileStatus> filesToProcess = status.getFiles().stream()
                        .filter(fileStatus -> fileStatus.getState() == FileState.RUNNING || fileStatus.getState() == FileState.QUEUED)
                        .sorted((o1, o2) -> (o1.getState() == o2.getState()) ? o1.getPathUri().compareTo(o2.getPathUri())
                                        : o2.getState().ordinal() - o1.getState().ordinal())
                        .collect(Collectors.toList());
        
        // then, process any unmarked/remaining files
        for (Status.FileStatus fileStatus : filesToProcess) {
            if (!processFile(fileStatus)) {
                status.setState(ReplayState.FAILED);
            } else if (status.getState() != ReplayState.RUNNING) {
                break;
            }
            
            statusCache.update(status);
        }
        
        // if we're still running, finish the replay
        if (status.getState() == ReplayState.RUNNING) {
            // finally, update our status as FINISHED
            if (status.getState() != ReplayState.FAILED)
                status.setState(ReplayState.FINISHED);
        }
        
        // status cache is updated here in the event that we stop gracefully
        statusCache.update(status);
    }
    
    private List<Status.FileStatus> listFiles(boolean replayUnfinished) {
        List<Status.FileStatus> fileStatuses = new ArrayList<>();
        
        // get the files/directories which match the pattern
        List<FileStatus> files = checkedGlobStatus(new Path(status.getPathUri()));
        
        // add the contents of any folders which matched the pattern
        files = files.stream().flatMap(x -> x.isDirectory() ? checkedGlobStatus(new Path(x.getPath(), "*")).stream().filter(FileStatus::isFile) : Stream.of(x))
                        .collect(Collectors.toList());
        
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            
            if (replayUnfinished && fileName.startsWith("_" + FileState.RUNNING)) {
                fileStatuses.add(new Status.FileStatus(file.getPath().toString(), FileState.RUNNING));
            } else if (replayUnfinished && fileName.startsWith("_" + FileState.QUEUED)) {
                fileStatuses.add(new Status.FileStatus(file.getPath().toString(), FileState.QUEUED));
            } else if (!file.getPath().getName().startsWith("_") && !file.getPath().getName().startsWith(".")) {
                Path queuedFile = renameFile(FileState.QUEUED, file.getPath());
                if (queuedFile != null) {
                    fileStatuses.add(new Status.FileStatus(queuedFile.toString(), FileState.QUEUED));
                } else {
                    log.warn("Unable to queue file \"{}\"", file.getPath());
                }
            }
        }
        
        return fileStatuses;
    }
    
    private List<FileStatus> checkedGlobStatus(Path path) {
        List<FileStatus> fileStatuses = Collections.emptyList();
        try {
            fileStatuses = Arrays.asList(filesystem.globStatus(path));
        } catch (Exception e) {
            log.warn("Encountered an error while listing files at [{}]", path.toUri().toString());
        }
        return fileStatuses;
    }
    
    private boolean processFile(Status.FileStatus fileStatus) {
        Path file = new Path(fileStatus.getPathUri());
        
        long numToSkip = 0;
        if (fileStatus.getState() == FileState.RUNNING) {
            
            numToSkip = fileStatus.getLinesRead();
        } else {
            
            Path runningFile = renameFile(FileState.RUNNING, file);
            if (runningFile != null) {
                file = runningFile;
            } else {
                log.error("Unable to rename file \"{}\" using prefix \"{}\"", file, FileState.RUNNING);
                fileStatus.setState(FileState.FAILED);
                return false;
            }
        }
        
        fileStatus.setPathUri(file.toString());
        fileStatus.setState(FileState.RUNNING);
        statusCache.update(status);
        
        boolean encounteredError = false;
        long linesRead = fileStatus.getLinesRead();
        long auditsSent = fileStatus.getAuditsSent();
        long auditsFailed = fileStatus.getAuditsFailed();
        long parseFailures = fileStatus.getParseFailures();
        
        BufferedReader reader = null;
        try {
            // read each audit message, and process via the audit service
            reader = new BufferedReader(new InputStreamReader(filesystem.open(file), UTF_8));
            TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>() {};
            
            String line;
            while (null != (line = reader.readLine()) && status.getState() == ReplayState.RUNNING) {
                if (++linesRead > numToSkip) {
                    try {
                        // send rate of 0 will pause the audit replay
                        long sendRate = status.getSendRate();
                        while (sendRate == 0) {
                            try {
                                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                            } catch (InterruptedException e) {
                                // not a problem if we exit a little early
                            }
                            sendRate = status.getSendRate();
                        }
                        
                        HashMap<String,String> auditParamsMap = mapper.readValue(line, typeRef);
                        
                        // add the audit replay id for tracking purposes
                        auditParamsMap.put("replayId", status.getId());
                        
                        if (!auditInternal(auditParamsMap)) {
                            log.warn("Failed to audit: {}", auditParamsMap.get(AUDIT_ID));
                            encounteredError = true;
                            auditsFailed++;
                        }
                        auditsSent++;
                        
                        try {
                            Thread.sleep((long) (1000.0 / sendRate));
                        } catch (InterruptedException e) {
                            // not a problem if we exit a little early
                        }
                    } catch (IOException e) {
                        log.warn("Unable to parse a JSON audit message from [{}]", line);
                        encounteredError = true;
                        parseFailures++;
                    }
                }
                
                // update the cached status per the status update interval
                if ((System.currentTimeMillis() - status.getLastUpdated().getTime()) > replayProperties.getStatusUpdateIntervalMillis()) {
                    fileStatus.setLinesRead(linesRead);
                    fileStatus.setAuditsSent(auditsSent);
                    fileStatus.setAuditsFailed(auditsFailed);
                    fileStatus.setParseFailures(parseFailures);
                    statusCache.update(status);
                }
            }
        } catch (IOException e) {
            encounteredError = true;
            log.error("Unable to read from file [{}]", file);
        }
        
        fileStatus.setLinesRead(linesRead);
        fileStatus.setAuditsSent(auditsSent);
        fileStatus.setAuditsFailed(auditsFailed);
        fileStatus.setParseFailures(parseFailures);
        
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                log.error("Unable to close file [{}]", file);
            }
        }
        
        if (status.getState() == ReplayState.RUNNING) {
            
            FileState fileState = (encounteredError) ? FileState.FAILED : FileState.FINISHED;
            Path finalPath = renameFile(fileState, file);
            
            if (finalPath != null) {
                fileStatus.setState(fileState);
                fileStatus.setPathUri(finalPath.toString());
            } else {
                fileStatus.setState(FileState.FAILED);
                log.error("Unable to rename file \"{}\" using prefix \"{}\"", file, fileState);
                return false;
            }
        }
        
        return true;
    }
    
    private Path renameFile(FileState newState, Path file) {
        String prefix = "_" + newState + ".";
        String fileName = file.getName();
        fileName = (fileName.startsWith("_")) ? prefix + fileName.substring(fileName.indexOf('.') + 1) : prefix + fileName;
        Path renamedFile = new Path(file.getParent(), fileName);
        try {
            if (filesystem.rename(file, renamedFile))
                return renamedFile;
        } catch (IOException e) {
            log.warn("Unable to rename file from \"{}\" using prefix \"{}\"", file, newState);
        }
        return null;
    }
    
    private boolean auditInternal(Map<String,String> auditParamsMap) {
        boolean success = false;
        try {
            success = audit(auditParamsMap);
        } catch (Exception e) {
            log.warn("Exception thrown while auditing: {}", auditParamsMap.get(AUDIT_ID), e);
        }
        return success;
    }
    
    abstract protected boolean audit(Map<String,String> auditParamsMap);
}
