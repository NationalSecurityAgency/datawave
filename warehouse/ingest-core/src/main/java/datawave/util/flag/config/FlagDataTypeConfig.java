package datawave.util.flag.config;

import java.util.List;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import datawave.ingest.input.reader.event.EventSequenceFileInputFormat;

/**
 *
 */
@XmlType
public class FlagDataTypeConfig {

    /*
     * somethingwave data name
     */
    private String dataName;
    /*
     * List of folders to scan for input files. The folder will be considered an absolute path if it leads with a slash, otherwise relative to the flagmakers
     * base directory
     */
    private List<String> folders;

    /*
     * Default to ESFIF but people can provide custom input format to override
     */
    private Class inputFormat = EventSequenceFileInputFormat.class;
    /*
     * Allows you to override reducer count for a particular datatype
     */
    private int reducers;

    /*
     * Allows you to override maximum flags/mappers/blocks for a particular datatype.
     */
    private int maxFlags;

    /* onehr, fivemin, etc */
    private String ingestPool;

    /*
     * Extra arguments to pass to the ingest process
     */
    private String extraIngestArgs;
    /*
     * Allows arguments to be passed to the distributor
     */
    private String distributionArgs = "none";
    // this does not get serialized or deserialized...

    // Should we process the data lifo or fifo (default fifo)
    // This is based on the file date, within a bucket
    private boolean lifo = false;

    /**
     * Allow you to override the timeout in secs. A flag file must be created within this timeout if any data exists to be processed.
     */
    private long timeoutMilliSecs = FlagMakerConfig.UNSET;

    /**
     * Allow you to override the flag count property.
     */
    // existing flag file count threshold afterwhich the timeoutMilliSecs is ignored. Default is -1 disabling this check.
    private int flagCountThreshold = FlagMakerConfig.UNSET;

    // The ingest scripts to put in the flag file
    private String script;

    // if set, then the files will be placed after the fileListMarker in the flag file, one per line
    // @see IngestJob -inputFileLists and -inputFileListMarker flags.
    private String fileListMarker = null;

    private String collectMetrics;

    @XmlTransient
    private long last = System.currentTimeMillis();

    public FlagDataTypeConfig(String dataName, List<String> folder, int reducers, String extraIngestArgs) {
        this.dataName = dataName;
        this.folders = folder;
        this.reducers = reducers;
        this.extraIngestArgs = extraIngestArgs;
    }

    public FlagDataTypeConfig() {}

    public String getIngestPool() {
        return ingestPool;
    }

    public void setIngestPool(String ingestPool) {
        this.ingestPool = ingestPool;
    }

    @XmlTransient
    public long getLast() {
        return last;
    }

    public void setLast(long last) {
        this.last = last;
    }

    public int getMaxFlags() {
        return maxFlags;
    }

    public void setMaxFlags(int maxFlags) {
        this.maxFlags = maxFlags;
    }

    public String getDataName() {
        return dataName;
    }

    public void setDataName(String dataName) {
        this.dataName = dataName;
    }

    public String getExtraIngestArgs() {
        return extraIngestArgs;
    }

    public void setExtraIngestArgs(String extraIngestArgs) {
        this.extraIngestArgs = extraIngestArgs;
    }

    public List<String> getFolder() {
        return folders;
    }

    public void setFolder(List<String> folder) {
        this.folders = folder;
    }

    public Class getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(Class inputFormat) {
        this.inputFormat = inputFormat;
    }

    public int getReducers() {
        return reducers;
    }

    public void setReducers(int reducers) {
        this.reducers = reducers;
    }

    public String getDistributionArgs() {
        return distributionArgs;
    }

    public void setDistributionArgs(String distributionArgs) {
        this.distributionArgs = distributionArgs;
    }

    public long getTimeoutMilliSecs() {
        return timeoutMilliSecs;
    }

    public void setTimeoutMilliSecs(long timeoutMilliSecs) {
        this.timeoutMilliSecs = timeoutMilliSecs;
    }

    public int getFlagCountThreshold() {
        return flagCountThreshold;
    }

    public void setFlagCountThreshold(int flagCountThreshold) {
        this.flagCountThreshold = flagCountThreshold;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public String getFileListMarker() {
        return fileListMarker;
    }

    public void setFileListMarker(String fileListMarker) {
        this.fileListMarker = fileListMarker;
    }

    public boolean isLifo() {
        return lifo;
    }

    public void setLifo(boolean lifo) {
        this.lifo = lifo;
    }

    public String getCollectMetrics() {
        return collectMetrics;
    }

    public boolean isCollectMetrics() {
        return collectMetrics != null ? (Boolean.valueOf(collectMetrics).booleanValue()) : false;
    }

    public void setCollectMetrics(String collectMetrics) {
        this.collectMetrics = collectMetrics;
    }
}
