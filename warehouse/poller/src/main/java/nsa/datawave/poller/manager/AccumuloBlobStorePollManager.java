package nsa.datawave.poller.manager;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.sadun.util.polling.CycleEndEvent;
import org.sadun.util.polling.CycleStartEvent;
import org.sadun.util.polling.DirectoryLookupEndEvent;
import org.sadun.util.polling.DirectoryLookupStartEvent;
import org.sadun.util.polling.DirectoryPoller;
import org.sadun.util.polling.FileFoundEvent;
import org.sadun.util.polling.FileMovedEvent;
import org.sadun.util.polling.FileSetFoundEvent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static nsa.datawave.poller.util.PollerUtils.getLastOptionValue;
import static nsa.datawave.poller.util.PollerUtils.getLastParsedOptionValue;

/**
 * Implementation of poll manager that places input files into Accumulo as blobs. A file may be split into multiple Accumulo keys.
 */
public class AccumuloBlobStorePollManager extends ConfiguredPollManager {
    private static final Logger log = Logger.getLogger(AccumuloBlobStorePollManager.class);
    
    private static final int DEFAULT_CHUNK_SIZE = 1048576;
    private static final String DEFAULT_FORMAT = "/{1,date,yyyy/MM/dd}/{0}";
    private static final long DEFAULT_LATENCY = 5000;
    private static final long DEFAULT_BUFFER = 1048576 * 2;
    private static final int DEFAULT_THREADS = 1;
    private static final String DEFAULT_COLUMN_VISIBILITY = "";
    
    private volatile boolean closed = false;
    private String dataType;
    private HashMap<String,String> errorDirs = new HashMap<>();
    private String archiveRawDir;
    private Connector connector;
    private ColumnVisibility columnVisibility;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private long batchWriteIntervalSize = DEFAULT_BUFFER / 2;
    
    private ByteBuffer copyBuffer;
    
    private String fileTable;
    private String indexTable;
    private BatchWriterConfig batchWriterConfig;
    private BatchWriter batchWriter;
    private BatchWriter indexBatchWriter;
    
    private String pathFormat = DEFAULT_FORMAT;
    private Option datatypeOpt, userOpt, passwordOpt, instanceOpt, zookeepersOpt, errorDirOpt, chunkSizeOpt, dirFormatOpt, tableNameOpt, indexTableNameOpt,
                    bwMaxLatencyOpt, bwMaxMemoryOpt, bwThreadsOpt, colVisOpt, keepRawOpt;
    
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
    
    @Override
    public Options getConfigurationOptions() {
        Options opt = super.getConfigurationOptions();
        
        datatypeOpt = new Option("t", "datatype", true, "Datatype");
        datatypeOpt.setRequired(true);
        datatypeOpt.setArgs(1);
        datatypeOpt.setType(String.class);
        opt.addOption(datatypeOpt);
        
        errorDirOpt = new Option("e", "errorDir", true, "Local error directory");
        errorDirOpt.setRequired(true);
        errorDirOpt.setArgs(1);
        errorDirOpt.setType(String.class);
        opt.addOption(errorDirOpt);
        
        userOpt = new Option("u", "username", true, "Accumulo user name");
        userOpt.setRequired(true);
        userOpt.setArgs(1);
        userOpt.setType(String.class);
        opt.addOption(userOpt);
        
        passwordOpt = new Option("p", "password", true, "Accumulo user password");
        passwordOpt.setRequired(true);
        passwordOpt.setArgs(1);
        passwordOpt.setType(String.class);
        opt.addOption(passwordOpt);
        
        instanceOpt = new Option("in", "instance", true, "Accumulo instance name");
        instanceOpt.setRequired(true);
        instanceOpt.setArgs(1);
        instanceOpt.setType(String.class);
        opt.addOption(instanceOpt);
        
        zookeepersOpt = new Option("zk", "zookeepers", true, "Accumulo Zookeepers (comma-separated list)");
        zookeepersOpt.setRequired(true);
        zookeepersOpt.setArgs(1);
        zookeepersOpt.setType(String.class);
        opt.addOption(zookeepersOpt);
        
        tableNameOpt = new Option("tn", "tableName", true, "Accumulo table name to which blobs are written");
        tableNameOpt.setRequired(true);
        tableNameOpt.setArgs(1);
        tableNameOpt.setType(String.class);
        opt.addOption(tableNameOpt);
        
        indexTableNameOpt = new Option("itn", "indexTableName", true, "Accumulo table name to which blob indices are written");
        indexTableNameOpt.setRequired(true);
        indexTableNameOpt.setArgs(1);
        indexTableNameOpt.setType(String.class);
        opt.addOption(indexTableNameOpt);
        
        chunkSizeOpt = new Option("cs", "chunkSize", true, "file chunking size in bytes, default is 1MB");
        chunkSizeOpt.setRequired(false);
        chunkSizeOpt.setArgs(1);
        chunkSizeOpt.setType(Integer.class);
        opt.addOption(chunkSizeOpt);
        
        dirFormatOpt = new Option("df", "dirFormat", true, "the directory format to use for storing files in Accumulo, "
                        + "using the MessageFormat class with the first val being the file name and second being the file modification date, " + "default is "
                        + DEFAULT_FORMAT);
        dirFormatOpt.setRequired(false);
        dirFormatOpt.setArgs(1);
        dirFormatOpt.setType(String.class);
        opt.addOption(dirFormatOpt);
        
        bwMaxLatencyOpt = new Option("ml", "maxLatency", true, "number of milliseconds to wait before flushing data to Accumulo");
        bwMaxLatencyOpt.setRequired(false);
        bwMaxLatencyOpt.setArgs(1);
        bwMaxLatencyOpt.setType(Long.class);
        opt.addOption(bwMaxLatencyOpt);
        
        bwMaxMemoryOpt = new Option("mm", "maxMemory", true, "number of bytes to buffer before flushing data to Accumulo");
        bwMaxMemoryOpt.setRequired(false);
        bwMaxMemoryOpt.setArgs(1);
        bwMaxMemoryOpt.setType(Long.class);
        opt.addOption(bwMaxMemoryOpt);
        
        bwThreadsOpt = new Option("wt", "writeThreads", true, "number of threads to use for writing data to Accumulo");
        bwThreadsOpt.setRequired(false);
        bwThreadsOpt.setArgs(1);
        bwThreadsOpt.setType(Integer.class);
        opt.addOption(bwThreadsOpt);
        
        colVisOpt = new Option("cv", "visibility", true, "column visibility to apply to input data");
        colVisOpt.setRequired(false);
        colVisOpt.setArgs(1);
        colVisOpt.setType(String.class);
        opt.addOption(colVisOpt);
        
        keepRawOpt = new Option("kr", "keepRaw", true, "keep raw files, moving them to the specified directory");
        keepRawOpt.setRequired(false);
        keepRawOpt.setArgs(1);
        keepRawOpt.setType(String.class);
        opt.addOption(keepRawOpt);
        
        return opt;
    }
    
    @Override
    public void configure(CommandLine cl) throws Exception {
        if (datatypeOpt == null) {
            throw new NullPointerException("datatype is null, please call getConfigured");
        }
        if (cl == null) {
            throw new NullPointerException("command line is null");
        }
        super.configure(cl);
        
        String[] dirs = cl.getOptionValues("q");
        String[] errorDirs = cl.getOptionValues(errorDirOpt.getOpt());
        if (dirs == null || errorDirs == null || errorDirs.length != dirs.length)
            throw new IllegalArgumentException("Need one error directory per queue directory.");
        for (int i = 0; i < errorDirs.length; ++i)
            this.errorDirs.put(dirs[i], errorDirs[i]);
        
        dataType = getLastOptionValue(cl, datatypeOpt.getOpt());
        pathFormat = getLastOptionValue(cl, dirFormatOpt.getOpt(), DEFAULT_FORMAT);
        archiveRawDir = getLastOptionValue(cl, keepRawOpt.getOpt());
        chunkSize = getLastParsedOptionValue(cl, chunkSizeOpt.getOpt(), chunkSizeOpt.getType(), DEFAULT_CHUNK_SIZE);
        
        columnVisibility = new ColumnVisibility(getLastParsedOptionValue(cl, colVisOpt.getOpt(), String.class, DEFAULT_COLUMN_VISIBILITY));
        
        // Open Accumulo connection
        String user = getLastOptionValue(cl, userOpt.getOpt());
        String pass = getLastOptionValue(cl, passwordOpt.getOpt());
        String instance = getLastOptionValue(cl, instanceOpt.getOpt());
        String zookeepers = getLastOptionValue(cl, zookeepersOpt.getOpt());
        connector = getConnector(user, pass, instance, zookeepers);
        
        // Set up a batch writer, creating the table if necessary
        fileTable = getLastOptionValue(cl, tableNameOpt.getOpt());
        if (!connector.tableOperations().exists(fileTable))
            connector.tableOperations().create(fileTable);
        
        indexTable = getLastOptionValue(cl, indexTableNameOpt.getOpt());
        if (!connector.tableOperations().exists(indexTable))
            connector.tableOperations().create(indexTable);
        
        int batchWriterThreads = getLastParsedOptionValue(cl, bwThreadsOpt.getOpt(), bwThreadsOpt.getType(), DEFAULT_THREADS);
        long batchWriterLatency = getLastParsedOptionValue(cl, bwMaxLatencyOpt.getOpt(), bwMaxLatencyOpt.getType(), DEFAULT_LATENCY);
        long batchWriterMemory = getLastParsedOptionValue(cl, bwMaxMemoryOpt.getOpt(), bwMaxMemoryOpt.getType(), DEFAULT_BUFFER);
        batchWriterConfig = new BatchWriterConfig();
        batchWriterConfig.setMaxLatency(batchWriterLatency, TimeUnit.MILLISECONDS);
        batchWriterConfig.setMaxMemory(batchWriterMemory);
        batchWriterConfig.setMaxWriteThreads(batchWriterThreads);
        
        // limit the size of the mutation to be half the writer's max memory
        batchWriteIntervalSize = batchWriterMemory / 2;
        
        copyBuffer = ByteBuffer.allocate(chunkSize);
        
        // Verify directories exist
        for (String errorDir : this.errorDirs.values()) {
            File errors = new File(errorDir);
            if (errors.isFile())
                throw new IllegalArgumentException("Errors directory " + errorDir + " is not a directory!");
            else if (!errors.exists() && !errors.mkdirs())
                throw new IllegalArgumentException("Unable to create errors directory " + errorDir);
            else if (!errors.canWrite())
                throw new IllegalArgumentException("Unable to write to errors directory " + errorDir);
        }
        
        if (archiveRawDir != null) {
            File archive = new File(archiveRawDir);
            if (archive.isFile())
                throw new IllegalArgumentException("Archive directory " + archiveRawDir + " is not a directory!");
            else if (!archive.exists() && !archive.mkdirs())
                throw new IllegalArgumentException("Unable to create archive directory " + archiveRawDir);
            else if (!archive.canWrite())
                throw new IllegalArgumentException("Unable to write to archive directory " + archiveRawDir);
        }
    }
    
    protected Connector getConnector(String user, String pass, String instance, String zookeepers) throws AccumuloSecurityException, AccumuloException {
        ClientConfiguration zkConfig = ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers);
        Instance zki = new ZooKeeperInstance(zkConfig);
        return zki.getConnector(user, new PasswordToken(pass));
    }
    
    @Override
    public void fileFound(FileFoundEvent event) {
        File file = event.getFile();
        String originalFile = file.getName();
        
        if (log.isDebugEnabled()) {
            log.debug("Processing " + originalFile + " by thread " + Thread.currentThread().getId());
        }
        
        long size = file.length();
        String sizeStr = Long.toString(size);
        String chunkNumStr = "";
        long timestamp = file.lastModified();
        
        String filePath = MessageFormat.format(pathFormat, file.getName(), new Date(timestamp));
        String hash = Integer.toHexString(filePath.hashCode()).toUpperCase();
        String fileRowKey = hash + ":" + file.getName();
        
        boolean error = false;
        Mutation m = new Mutation(fileRowKey);
        FileInputStream fis = null;
        FileChannel channel;
        
        try {
            initializeBatchWritersIfNecessary();
            
            fis = new FileInputStream(file);
            channel = fis.getChannel();
            
            long processed = 0;
            int chunkNumber = 1;
            for (boolean eof = false; !eof; processed += copyBuffer.limit(), chunkNumber++) {
                
                eof = readChunk(channel, copyBuffer);
                
                if (size > chunkSize) { // No col qual if the file fits in one chunk
                    // 0-pad the chunk number to ensure numeric sorting in Accumulo (10 is the max digits for an int in Java)
                    chunkNumStr = String.format("%010d", chunkNumber);
                }
                m.put(sizeStr, chunkNumStr, columnVisibility, timestamp, new Value(copyBuffer));
                if ((m.size() * chunkSize) > batchWriteIntervalSize) {
                    batchWriter.addMutation(m);
                    m = new Mutation(fileRowKey);
                }
            }
            
            if (m.size() > 0) {
                batchWriter.addMutation(m);
            }
            if (processed < size)
                throw new IOException("Did not read enough bytes for " + file.getAbsolutePath() + ". Expected " + size + " but read " + processed);
            
            Mutation indexMutation = new Mutation(filePath);
            indexMutation.put(hash, sizeStr, columnVisibility, timestamp, "");
            indexBatchWriter.addMutation(indexMutation);
            
            if (log.isTraceEnabled())
                log.trace("Loaded file " + file.getAbsolutePath() + " of size " + size + " into Accumulo using " + (chunkNumber - 1) + " chunks.");
        } catch (IOException e) {
            error = true;
            log.error("Error reading input file " + file, e);
        } catch (MutationsRejectedException e) {
            error = true;
            log.error("Unable to add chunk of file " + file + " to Accumulo: " + e.getMessage(), e);
            batchWriterError();
        } catch (TableNotFoundException e) {
            error = true;
            log.error("Unable to add chunk of file " + file + " to Accumulo: " + e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
        
        if (!error) {
            addReceivedFileReport(originalFile, file);
            fileFinished(file);
        } else {
            fileError(file, event.getPoller());
        }
    }
    
    protected boolean readChunk(FileChannel channel, ByteBuffer buffer) throws IOException {
        boolean eof = false;
        buffer.clear();
        
        for (int read = 0; read >= 0 && buffer.hasRemaining();) {
            read = channel.read(buffer);
            eof = (read < 0) || (channel.position() >= channel.size() - 1);
        }
        
        buffer.flip();
        return eof;
    }
    
    protected void fileFinished(File file) {
        String localErrorDir = errorDirs.get(file.getPath());
        
        // Flush everything to Accumulo and delete the work file if successful
        try {
            batchWriter.flush();
            indexBatchWriter.flush();
            
            // Attempt to save off the original file if we are configured that way
            if (archiveRawDir != null) {
                File archiveFile = new File(archiveRawDir, file.getName());
                if (!file.renameTo(archiveFile)) {
                    File errorFile = new File(localErrorDir, file.getName() + ".archive");
                    if (!file.renameTo(errorFile)) {
                        log.error("Unable to rename completed input file: " + file.getAbsolutePath() + " to " + errorFile.getAbsolutePath());
                    } else {
                        log.info("Unable to archive completed file: " + errorFile.getAbsolutePath() + " should be archived by hand");
                    }
                }
            }
            // Otherwise, simply delete the raw file
            else {
                boolean deleted = file.delete();
                if (!deleted) {
                    File errorFile = new File(localErrorDir, file.getName() + ".delete");
                    if (!file.renameTo(errorFile)) {
                        log.error("Unable to delete or rename completed input file: " + file.getAbsolutePath());
                    } else {
                        log.info("Unable to delete completed file: " + errorFile.getAbsolutePath() + " can be deleted");
                    }
                }
            }
        } catch (MutationsRejectedException e) {
            log.error("Unable to write file " + file.getAbsolutePath() + " to Accumulo: " + e.getMessage(), e);
            if (!file.renameTo(new File(localErrorDir, file.getName()))) {
                log.error("Error moving file " + file.getAbsolutePath() + " to " + new File(localErrorDir, file.getName()));
            }
            batchWriterError();
        }
    }
    
    protected void initializeBatchWritersIfNecessary() throws TableNotFoundException {
        if (batchWriter == null) {
            batchWriter = connector.createBatchWriter(fileTable, batchWriterConfig);
        }
        if (indexBatchWriter == null) {
            indexBatchWriter = connector.createBatchWriter(indexTable, batchWriterConfig);
        }
    }
    
    protected void batchWriterError() {
        try {
            batchWriter.close();
        } catch (MutationsRejectedException e1) {
            // ignore -- we already marked the file as an error
        }
        batchWriter = null;
        
        try {
            indexBatchWriter.close();
        } catch (MutationsRejectedException e) {
            // ignore -- we already marked the file as an error
        }
        indexBatchWriter = null;
    }
    
    protected void fileError(File file, DirectoryPoller poller) {
        String localErrorDir = errorDirs.get(file.getParent());
        
        // if we are in the process of closing, then assume the error is because of that and not an
        // actual error. If it is an actual processing error, then this will be realized the next
        // time through when we re-process the file.
        if (!poller.isShuttingDown() && !closed) {
            // Move the current file to error directory.
            if (!file.renameTo(new File(localErrorDir, file.getName()))) {
                log.error("Error moving file " + file.getAbsolutePath() + " to " + new File(localErrorDir, file.getName()));
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        // Prevent calling this twice.
        if (closed)
            return;
        
        closed = true;
        
        try {
            if (batchWriter != null) {
                batchWriter.close();
            }
        } catch (MutationsRejectedException e) {
            log.error("Error closing batch writer: " + e.getMessage(), e);
        }
        try {
            if (indexBatchWriter != null) {
                indexBatchWriter.close();
            }
        } catch (MutationsRejectedException e) {
            log.error("Error closing index batch writer: " + e.getMessage(), e);
        }
        
        writeProvenanceReports();
        super.close();
    }
    
    @Override
    public void cycleStarted(CycleStartEvent cycleStartEvent) {
        if (log.isTraceEnabled())
            log.trace("Poll cycle started");
    }
    
    @Override
    public void cycleEnded(CycleEndEvent cycleEndEvent) {
        if (log.isTraceEnabled())
            log.trace("Poll cycle ended");
    }
    
    @Override
    public void directoryLookupStarted(DirectoryLookupStartEvent directoryLookupStartEvent) {
        if (log.isTraceEnabled())
            log.trace("directory lookup started");
    }
    
    @Override
    public void directoryLookupEnded(DirectoryLookupEndEvent directoryLookupEndEvent) {
        if (log.isTraceEnabled())
            log.trace("directory lookup ended");
    }
    
    @Override
    public void fileSetFound(FileSetFoundEvent fileSetFoundEvent) {}
    
    @Override
    public void exceptionMovingFile(File src, File dst) {
        log.error("Error moving file: " + src.getAbsolutePath() + " to " + dst.getAbsolutePath());
    }
}
