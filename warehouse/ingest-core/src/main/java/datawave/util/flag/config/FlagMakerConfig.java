package datawave.util.flag.config;

import datawave.util.flag.processor.DateUtils;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple JAXB Wrapper for FlagConfig objects
 */
@XmlRootElement
public class FlagMakerConfig {
    
    // A value used as a default to denote that it was unset
    public static int UNSET = Integer.MIN_VALUE;
    
    @XmlElement
    private List<FlagDataTypeConfig> flagCfg = new ArrayList<>();
    
    private FlagDataTypeConfig defaultCfg;
    // default to localhost
    private String hdfs = "hdfs://localhost:9000";
    
    private String datawaveHome;
    // default path
    private String baseHDFSDir = "/data/ShardIngest";
    private int socketPort;
    private String flagFileDirectory;
    
    // default to a year, month, day pattern
    private String filePattern = "2*/*/*/*";
    // default to simple. valid values are simple|date|folderdate
    private String distributorType = "simple";
    // default timeout
    private long timeoutMilliSecs = (5L * DateUtils.A_MINUTE);
    // default sleep between cycles
    private long sleepMilliSecs = 15000L;
    // existing flag file count threshold afterwhich the timeoutMilliSecs is ignored. Default is -1 disabling this check.
    protected int flagCountThreshold = UNSET;
    // max flag file length
    private int maxFileLength = Integer.MAX_VALUE;
    // should we set the timestamp on flag files to the last timestamp of the file contained therein
    private boolean setFlagFileTimestamp = true;
    // use folder for file timestamp instead of the actual file timestamp
    private boolean useFolderTimestamp = false;
    // location of where to store flagmaker metrics
    private String flagMetricsDirectory = "/data/BulkIngest/FlagMakerMetrics";
    // number of threads for the flag maker lookup
    protected int maxHdfsThreads = 25;
    // maximum cache of HDFS directory cache size
    protected int directoryCacheSize = 2000;
    // directory cache timeout. Default is 2 Hours
    protected long directoryCacheTimeout = (2 * 60 * 60 * 1000);
    
    public FlagDataTypeConfig getDefaultCfg() {
        return defaultCfg;
    }
    
    public void setDefaultCfg(FlagDataTypeConfig defaultCfg) {
        this.defaultCfg = defaultCfg;
    }
    
    public String getDatawaveHome() {
        return datawaveHome;
    }
    
    public void setDatawaveHome(String datawaveHome) {
        this.datawaveHome = datawaveHome;
    }
    
    public String getDistributorType() {
        return distributorType;
    }
    
    public void setDistributorType(String distributorType) {
        this.distributorType = distributorType;
    }
    
    public String getFilePattern() {
        return filePattern;
    }
    
    public void setFilePattern(String filePattern) {
        this.filePattern = filePattern;
    }
    
    public String getBaseHDFSDir() {
        return baseHDFSDir;
    }
    
    public void setBaseHDFSDir(String baseHDFSDir) {
        this.baseHDFSDir = baseHDFSDir;
    }
    
    public String getFlagFileDirectory() {
        return flagFileDirectory;
    }
    
    public void setFlagFileDirectory(String flagFileDirectory) {
        this.flagFileDirectory = flagFileDirectory;
    }
    
    public String getHdfs() {
        return hdfs;
    }
    
    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }
    
    public int getSocketPort() {
        return socketPort;
    }
    
    public void setSocketPort(int socketPort) {
        this.socketPort = socketPort;
    }
    
    public long getTimeoutMilliSecs() {
        return timeoutMilliSecs;
    }
    
    public void setTimeoutMilliSecs(long timeoutMilliSecs) {
        this.timeoutMilliSecs = timeoutMilliSecs;
    }
    
    public long getSleepMilliSecs() {
        return sleepMilliSecs;
    }
    
    public void setSleepMilliSecs(long sleepMilliSecs) {
        this.sleepMilliSecs = sleepMilliSecs;
    }
    
    public int getMaxHdfsThreads() {
        return maxHdfsThreads;
    }
    
    public void setMaxHdfsThreads(int maxHdfsThreads) {
        this.maxHdfsThreads = maxHdfsThreads;
    }
    
    public int getDirectoryCacheSize() {
        return directoryCacheSize;
    }
    
    public void setDirectoryCacheSize(int maxDirectoryCacheSize) {
        this.directoryCacheSize = maxDirectoryCacheSize;
    }
    
    public long getDirectoryCacheTimeout() {
        return directoryCacheTimeout;
    }
    
    public void setDirectoryCacheTimeout(int directoryCacheTimeout) {
        this.directoryCacheTimeout = directoryCacheTimeout;
    }
    
    public int getMaxFileLength() {
        return maxFileLength;
    }
    
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }
    
    public boolean isSetFlagFileTimestamp() {
        return setFlagFileTimestamp;
    }
    
    public void setSetFlagFileTimestamp(boolean setFlagFileTimestamp) {
        this.setFlagFileTimestamp = setFlagFileTimestamp;
    }
    
    public boolean isUseFolderTimestamp() {
        return useFolderTimestamp;
    }
    
    public void setUseFolderTimestamp(boolean useFolderTimestamp) {
        this.useFolderTimestamp = useFolderTimestamp;
    }
    
    public int getFlagCountThreshold() {
        return flagCountThreshold;
    }
    
    public void setFlagCountThreshold(int flagCountThreshold) {
        this.flagCountThreshold = flagCountThreshold;
    }
    
    /**
     * Gets the list of <code>FlagConfig</code>s
     *
     * @return
     */
    @XmlTransient
    public List<FlagDataTypeConfig> getFlagConfigs() {
        return flagCfg;
    }
    
    /**
     * Allows the programmatic addition of <code>FlagConfig</code>s to this object.
     *
     * @param fc
     */
    public void addFlagConfig(FlagDataTypeConfig fc) {
        if (fc != null) {
            flagCfg.add(fc);
        }
    }
    
    public void setFlagMetricsDirectory(String d) {
        this.flagMetricsDirectory = d;
    }
    
    public String getFlagMetricsDirectory() {
        return flagMetricsDirectory;
    }
}
