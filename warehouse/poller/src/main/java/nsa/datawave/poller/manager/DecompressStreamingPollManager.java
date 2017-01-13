package nsa.datawave.poller.manager;

import java.io.*;

import java.net.InetAddress;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.*;

import org.apache.log4j.Logger;
import org.sadun.util.polling.CycleEndEvent;
import org.sadun.util.polling.CycleStartEvent;
import org.sadun.util.polling.DirectoryLookupEndEvent;
import org.sadun.util.polling.DirectoryLookupStartEvent;
import org.sadun.util.polling.FileFoundEvent;
import org.sadun.util.polling.FileMovedEvent;
import org.sadun.util.polling.FileSetFoundEvent;

/**
 * A manager that will move files to HDFS.
 * 
 * This is a beta, and several cases involving shut down/start still need to be addressed.
 */
public class DecompressStreamingPollManager extends ConfiguredPollManager implements RecoverablePollManager {
    private static final Logger log = Logger.getLogger(DecompressStreamingPollManager.class);
    
    private String hdfsDestDir, dataType, hostName;
    
    private Option hdfsDestDirOpt, dataTypeOpt, archiveDir, archiveRawFlag;
    protected String localArchiveDir = null;
    protected boolean archiveRaw = false;
    
    private FileSystem fs;
    
    private Calendar calendar = new GregorianCalendar();
    
    @Override
    public void exceptionDeletingTargetFile(File f) {
        log.error("Error deleting file: " + f.getAbsolutePath());
    }
    
    @Override
    public void fileMoved(FileMovedEvent event) {
        if (log.isTraceEnabled()) {
            log.trace("File " + event.getOriginalPath() + " moved to " + event.getPath());
        }
    }
    
    @Override
    public String getDatatype() {
        return dataType;
    }
    
    /**
     * Gets the data type and destination directory in HDFS for the file.
     */
    @Override
    public Options getConfigurationOptions() {
        Options opt = super.getConfigurationOptions();
        
        dataTypeOpt = new Option("t", "dataType", true, "Datatype");
        dataTypeOpt.setRequired(true);
        dataTypeOpt.setArgs(1);
        dataTypeOpt.setType(String.class);
        opt.addOption(dataTypeOpt);
        
        hdfsDestDirOpt = new Option("h", "hdfsDestDir", true, "Base hdfsDestDirOptination directory in HDFS");
        hdfsDestDirOpt.setRequired(true);
        hdfsDestDirOpt.setArgs(1);
        hdfsDestDirOpt.setType(String.class);
        opt.addOption(hdfsDestDirOpt);
        
        // throwaway options used to make the calling scripts work
        Option errorDirs = new Option("e", "errorDir", true, "Error directory (unused)");
        errorDirs.setRequired(false);
        errorDirs.setArgs(1);
        errorDirs.setType(String.class);
        opt.addOption(errorDirs);
        
        Option workDir = new Option("w", "workDir", true, "local work directory");
        workDir.setRequired(true);
        workDir.setArgs(1);
        workDir.setType(String.class);
        opt.addOption(workDir);
        
        Option completedDir = new Option("r", "completedDir", true, "directory for completed files that have not been copied into HDFS.");
        completedDir.setRequired(true);
        completedDir.setArgs(1);
        completedDir.setType(String.class);
        opt.addOption(completedDir);
        
        Option latency = new Option("l", "latency", true, "ms to wait before closing the output file");
        latency.setRequired(true);
        latency.setArgs(1);
        latency.setType(Long.class);
        opt.addOption(latency);
        
        Option bogus = new Option("b", "latency", true, "ms to wait before closing the output file");
        bogus.setRequired(true);
        bogus.setArgs(1);
        bogus.setType(Long.class);
        opt.addOption(bogus);
        
        bogus = new Option("mf", "latency", true, "ms to wait before closing the output file");
        opt.addOption(bogus);
        
        archiveDir = new Option("a", "archiveDir", true, "directory for archived raw files if the archiveRaw flag is supplied");
        archiveDir.setRequired(false);
        archiveDir.setArgs(1);
        archiveDir.setType(String.class);
        opt.addOption(archiveDir);
        
        archiveRawFlag = new Option("ar", "archiveRaw", false, "if supplied, files are archived to the specified archive directory");
        opt.addOption(archiveRawFlag);
        
        return opt;
    }
    
    @Override
    public void configure(CommandLine cl) throws Exception {
        super.configure(cl);
        dataType = cl.getOptionValue(dataTypeOpt.getOpt());
        hdfsDestDir = cl.getOptionValue(hdfsDestDirOpt.getOpt());
        
        this.localArchiveDir = getLastOptionValue(cl, archiveDir.getOpt());
        this.archiveRaw = cl.hasOption(archiveRawFlag.getOpt());
        
        if (archiveRaw) {
            if (this.localArchiveDir == null) {
                throw new RuntimeException("Specified that raw files are to be archived, but not archive directory supplied");
            }
            File dir = new File(this.localArchiveDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (!dir.canWrite()) {
                throw new RuntimeException("Unable to write to archive directory: " + this.localArchiveDir);
            }
        }
        
        Configuration conf = new Configuration(true);
        conf.set("fs.defaultFS", hdfsDestDir);
        fs = FileSystem.get(conf);
        
        log.info("HDFS FileSystem configured: " + fs.getUri());
        
        hostName = InetAddress.getLocalHost().getHostName();
    }
    
    /**
     * A convenience method to get the last specified value on the command line for an option instead of the first which CommandLine.getOptionValue returns.
     * 
     * @param cl
     * @param opt
     * @return The last option value for opt
     */
    protected String getLastOptionValue(CommandLine cl, String opt) {
        String[] values = cl.getOptionValues(opt);
        
        return (values == null) ? null : values[values.length - 1];
    }
    
    /**
     * This method attempts to move the file marked by the <code>FileFoundEvent</code> into HDFS.
     * 
     * This method decompresses the source file, writes it to /tmp in HDFS, and then moves the file to the appropriate directory to be picked up by the flag
     * maker. This logic is necessary because writing to the stream does not result in an atomic creation of the file.
     * 
     */
    public void fileFound(FileFoundEvent event) {
        File file = event.getFile();
        
        final File evtFile = event.getFile();
        String originalFile = evtFile.getName();
        
        Path dfsPath = createHdfsPath();
        Path dfsTmpWrite = new Path("/tmp/" + dfsPath.getName());
        
        if (log.isDebugEnabled()) {
            log.debug("Received file " + file + ", moving to " + dfsPath);
        }
        
        try {
            Writer out = new BufferedWriter(new OutputStreamWriter(fs.create(dfsTmpWrite)));
            
            Reader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(new Path(file.toURI().toString()).toUri())))));
            
            while (in.ready()) {
                out.write(in.read());
            }
            
            in.close();
            out.close();
            
            boolean newDir = ensureDestinationDirectoryExists(dfsPath);
            if (newDir)
                log.info("Created new directory " + dfsPath.getParent());
            
            fs.rename(dfsTmpWrite, dfsPath);
            
            if (archiveRaw) {
                if (!file.renameTo(new File(localArchiveDir, file.getName()))) {
                    log.error("Unable to rename completed input file: " + file.getAbsolutePath());
                }
            } else if (!file.delete()) {
                log.error("Could not delete input file " + file + "!!");
            }
            addReceivedFileReport(originalFile, file);
        } catch (IOException e) {
            log.error("IOException while copying file.", e);
        }
    }
    
    /**
     * Creates a path for the file on HDFS that adheres to the <code>HDFS_BASE/DATA_TYPE/YEAR/MONTH/DAY/file</code> format.
     */
    public Path createHdfsPath() {
        String year, month, day;
        synchronized (this.calendar) { // is this necessary?
            this.calendar.setTimeInMillis(System.currentTimeMillis());
            year = String.format("%04d", this.calendar.get(Calendar.YEAR));
            month = String.format("%02d", this.calendar.get(Calendar.MONTH) + 1);
            day = String.format("%02d", this.calendar.get(Calendar.DATE));
        }
        String destFilePath = this.hdfsDestDir
                        + '/'
                        + dataType
                        + '/'
                        + year
                        + '/'
                        + month
                        + '/'
                        + day
                        + '/'
                        + String.format("%s_%s%s%s%02d%02d%02d_%s", this.dataType, year, month, day, this.calendar.get(Calendar.HOUR_OF_DAY),
                                        this.calendar.get(Calendar.MINUTE), this.calendar.get(Calendar.SECOND), this.hostName);
        return new Path(destFilePath);
    }
    
    /* Returns true if a new directory path was created; false otherwise. */
    private boolean ensureDestinationDirectoryExists(Path p) throws IOException {
        Path parent = p.getParent();
        if (parent != null && !fs.exists(parent)) {
            return fs.mkdirs(parent);
        } else {
            return false;
        }
    }
    
    @Override
    public void close() throws IOException {
        fs.close();
        writeProvenanceReports();
        super.close();
    }
    
    public void directoryLookupStarted(DirectoryLookupStartEvent event) {}
    
    public void fileSetFound(FileSetFoundEvent arg0) {}
    
    public void exceptionMovingFile(File arg0, File arg1) {}
    
    public void recover(File queueDir) {}
    
    public void cycleEnded(CycleEndEvent arg0) {}
    
    public void cycleStarted(CycleStartEvent arg0) {}
    
    public void directoryLookupEnded(DirectoryLookupEndEvent arg0) {}
    
}
