package nsa.datawave.ingest.util;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * 
 */
public class IngestFileReport {
    
    public enum Status {
        RECEIVE, SPAWN, STORE
    };
    
    private static final Logger log = Logger.getLogger(IngestFileReport.class);
    
    private String fileName = null;
    private List<String> sourceFileNames = new ArrayList<>();
    private Long fileSize = null;
    private Status status = null;
    private Date eventTimestamp = null;
    private String transitURI = null;
    
    public IngestFileReport() {
        
    }
    
    public IngestFileReport(String[] data) {
        
        if (data.length >= 1 && StringUtils.isNotBlank(data[0])) {
            eventTimestamp = new Date(Long.valueOf(data[0]));
        }
        
        if (data.length >= 2 && StringUtils.isNotBlank(data[1])) {
            status = Status.valueOf(data[1]);
        }
        
        if (data.length >= 3 && StringUtils.isNotBlank(data[2])) {
            fileName = data[2];
        }
        
        if (data.length >= 4 && StringUtils.isNotBlank(data[3])) {
            fileSize = Long.valueOf(data[3]);
        }
        if (data.length >= 5 && StringUtils.isNotBlank(data[4])) {
            String[] filenames = StringUtils.split(data[4], ':');
            Collections.addAll(sourceFileNames, filenames);
        }
        
        if (data.length >= 6 && StringUtils.isNotBlank(data[5])) {
            transitURI = data[5];
        }
    }
    
    public String getFileName() {
        return fileName;
    }
    
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
    public Long getFileSize() {
        return fileSize;
    }
    
    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    public Date getEventTimestamp() {
        return eventTimestamp;
    }
    
    public void setEventTimestamp(Date eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }
    
    public List<String> getSourceFileNames() {
        return sourceFileNames;
    }
    
    public void setSourceFileNames(List<String> sourceFileNames) {
        this.sourceFileNames.clear();
        this.sourceFileNames = sourceFileNames;
    }
    
    public void addSourceFile(String sourceFile) {
        this.sourceFileNames.add(sourceFile);
    }
    
    public String[] getData() {
        
        String[] data = new String[6];
        data[0] = (eventTimestamp == null) ? "" : Long.toString(eventTimestamp.getTime());
        data[1] = (status == null) ? "" : status.toString();
        data[2] = (fileName == null) ? "" : fileName;
        data[3] = (fileSize == null) ? "" : fileSize.toString();
        data[4] = (sourceFileNames == null || sourceFileNames.size() == 0) ? "" : StringUtils.join(sourceFileNames, ":");
        data[5] = (transitURI == null) ? "" : transitURI;
        return data;
    }
    
    public String getTransitURI() {
        return transitURI;
    }
    
    public void setTransitURI(String transitURI) {
        this.transitURI = transitURI;
    }
    
    static public void writeCsv(File localFile, List<IngestFileReport> ingestFileReportList) throws IOException {
        
        CSVWriter writer = null;
        try {
            
            if (localFile.exists() == false) {
                localFile.createNewFile();
            }
            // make sure all users can read/write so that the file can be collected and moved
            localFile.setReadable(true, false);
            localFile.setWritable(true, false);
            writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(localFile)));
            for (IngestFileReport report : ingestFileReportList) {
                writer.writeNext(report.getData());
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            throw e;
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        }
    }
}
