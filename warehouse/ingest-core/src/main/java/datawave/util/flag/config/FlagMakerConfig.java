package datawave.util.flag.config;

import datawave.util.StringUtils;
import datawave.util.flag.FlagMaker;
import datawave.util.flag.processor.DateFlagDistributor;
import datawave.util.flag.processor.DateFolderFlagDistributor;
import datawave.util.flag.processor.DateUtils;
import datawave.util.flag.processor.FlagDistributor;
import datawave.util.flag.processor.SimpleFlagDistributor;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Simple JAXB Wrapper for FlagConfig objects
 */
@XmlRootElement
public class FlagMakerConfig {
    
    // A value used as a default to denote that it was unset
    public static final int UNSET = Integer.MIN_VALUE;
    
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
    
    public static final String DEFAULT_FILE_PATTERN = "2*/*/*/*";
    
    // a list of file patterns.
    @XmlElement(name = "filePattern")
    private List<String> filePatterns = new ArrayList<>();
    
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
    // implementation of flagmaker to run
    private String flagMakerClass = FlagMaker.class.getName();
    
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
    
    public List<String> getFilePatterns() {
        return (filePatterns.isEmpty() ? Collections.singletonList(DEFAULT_FILE_PATTERN) : filePatterns);
    }
    
    public void addFilePattern(String filePattern) {
        this.filePatterns.add(filePattern);
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
    
    public String getFlagMakerClass() {
        return flagMakerClass;
    }
    
    public void setFlagMakerClass(String flagMakerClass) {
        this.flagMakerClass = flagMakerClass;
    }
    
    /**
     * Gets the list of <code>FlagConfig</code>s
     *
     * @return a list of flagconfigs
     */
    @XmlTransient
    public List<FlagDataTypeConfig> getFlagConfigs() {
        return flagCfg;
    }
    
    /**
     * Allows the programmatic addition of <code>FlagConfig</code>s to this object.
     *
     * @param fc
     *            the flag config
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
    
    public FlagDistributor getFlagDistributor() {
        FlagDistributor fd = null;
        if ("simple".equals(this.distributorType)) {
            fd = new SimpleFlagDistributor();
        } else if ("date".equals(this.distributorType)) {
            fd = new DateFlagDistributor();
        } else if ("folderdate".equals(this.distributorType)) {
            fd = new DateFolderFlagDistributor();
        }
        
        return fd;
    }
    
    /**
     * Validate config and set up folders for each data type. Here we have a few rules:
     * <ul>
     * <li>if you provide no folders, then we will assume that the folder is the data type which will be appended to the base directory (e.g.
     * /data/ShardIngest).</li>
     * <li>Users can provide absolute file paths by leading with a slash ("/")</li>
     * </ul>
     */
    public void validate() {
        String prefix = this.getClass().getSimpleName() + " Error: ";
        // validate the config
        if (this.defaultCfg.getScript() == null) {
            throw new IllegalArgumentException(prefix + "default script is required");
        }
        if (this.baseHDFSDir == null) {
            throw new IllegalArgumentException(prefix + "baseHDFSDir is required");
        }
        
        if (!this.baseHDFSDir.endsWith("/")) {
            setBaseHDFSDir(this.baseHDFSDir + "/");
        }
        
        if (this.socketPort < 1025 || socketPort > 65534) {
            throw new IllegalArgumentException(prefix + "socketPort is required and must be greater than 1024 and less than 65535");
        }
        
        if (this.flagFileDirectory == null) {
            throw new IllegalArgumentException(prefix + "flagFileDirectory is required");
        }
        
        if (this.defaultCfg.getMaxFlags() < 1) {
            throw new IllegalArgumentException(prefix + "Default Max Flags must be set.");
        }
        
        if (this.distributorType == null || !this.distributorType.matches("(simple|date|folderdate)")) {
            throw new IllegalArgumentException(
                            "Invalid Distributor type provided: " + this.distributorType + ". Must be one of the following: simple|date|folderdate");
        }
        
        for (FlagDataTypeConfig cfg : this.flagCfg) {
            if (cfg.getInputFormat() == null)
                throw new IllegalArgumentException("Input Format Class must be specified for data type: " + cfg.getDataName());
            if (cfg.getIngestPool() == null)
                throw new IllegalArgumentException("Ingest Pool must be specified for data type: " + cfg.getDataName());
            if (cfg.getFlagCountThreshold() == FlagMakerConfig.UNSET) {
                cfg.setFlagCountThreshold(this.flagCountThreshold);
            }
            if (cfg.getTimeoutMilliSecs() == FlagMakerConfig.UNSET) {
                cfg.setTimeoutMilliSecs(this.timeoutMilliSecs);
            }
            cfg.setLast(System.currentTimeMillis() + cfg.getTimeoutMilliSecs());
            if (cfg.getMaxFlags() < 1) {
                cfg.setMaxFlags(this.defaultCfg.getMaxFlags());
            }
            if (cfg.getReducers() < 1) {
                cfg.setReducers(this.defaultCfg.getReducers());
            }
            if (cfg.getScript() == null || "".equals(cfg.getScript())) {
                cfg.setScript(this.defaultCfg.getScript());
            }
            if (cfg.getFileListMarker() == null || "".equals(cfg.getFileListMarker())) {
                cfg.setFileListMarker(this.defaultCfg.getFileListMarker());
            }
            if (cfg.getFileListMarker() != null) {
                if (cfg.getFileListMarker().indexOf(' ') >= 0) {
                    throw new IllegalArgumentException(prefix + "fileListMarker cannot contain spaces");
                }
            }
            if (cfg.getCollectMetrics() == null || "".equals(cfg.getCollectMetrics())) {
                cfg.setCollectMetrics(this.defaultCfg.getCollectMetrics());
            }
            List<String> folders = cfg.getFolder();
            if (folders == null || folders.isEmpty()) {
                folders = new ArrayList<>();
                cfg.setFolder(folders);
                // add the default path. we'll bomb later if it's not there.
                folders.add(cfg.getDataName());
            }
            List<String> fixedFolders = new ArrayList<>();
            for (int i = 0; i < folders.size(); i++) {
                for (String folder : StringUtils.split(folders.get(i), ',')) {
                    folder = folder.trim();
                    // let someone specify an absolute path.
                    if (!folder.startsWith("/")) {
                        fixedFolders.add(this.baseHDFSDir + folder);
                    } else {
                        fixedFolders.add(folder);
                    }
                }
            }
            cfg.setFolder(fixedFolders);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("hdfs: " + this.getHdfs() + "\n");
        result.append("datawaveHome: " + this.getDatawaveHome() + "\n");
        result.append("baseHDFSDir: " + this.getBaseHDFSDir() + "\n");
        result.append("socketPort: " + this.getSocketPort() + "\n");
        result.append("flagFileDirectory: " + this.getFlagFileDirectory() + "\n");
        result.append("filePatterns: " + this.getFilePatterns() + "\n");
        result.append("distributorType: " + this.getDistributorType() + "\n");
        result.append("timeoutMilliSecs: " + this.getTimeoutMilliSecs() + "\n");
        result.append("sleepMilliSecs: " + this.getSleepMilliSecs() + "\n");
        result.append("flagCountThreshold: " + this.getFlagCountThreshold() + "\n");
        result.append("maxFileLength: " + this.getMaxFileLength() + "\n");
        result.append("isSetFlagFileTimestamp: " + this.isSetFlagFileTimestamp() + "\n");
        result.append("useFolderTimestamp: " + this.isUseFolderTimestamp() + "\n");
        result.append("flagMetricsDirectory: " + this.getFlagMetricsDirectory() + "\n");
        result.append("maxHdfsThreads: " + this.getMaxHdfsThreads() + "\n");
        result.append("directoryCacheSize: " + this.getDirectoryCacheSize() + "\n");
        result.append("directoryCacheTimeout: " + this.getDirectoryCacheTimeout() + "\n");
        result.append("flagMakerClass: " + this.getFlagMakerClass() + "\n");
        return result.toString();
    }
}
