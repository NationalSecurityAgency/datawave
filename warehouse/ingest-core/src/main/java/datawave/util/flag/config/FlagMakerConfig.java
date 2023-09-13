package datawave.util.flag.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.util.flag.FlagMaker;
import datawave.util.flag.processor.DateUtils;
import datawave.util.flag.processor.SimpleFlagDistributor;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * Simple JAXB Wrapper for FlagConfig objects
 */
@XmlRootElement
@Data
public class FlagMakerConfig {

    // A value used as a default to denote that it was unset
    public static final int UNSET = Integer.MIN_VALUE;

    @XmlElement
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private List<FlagDataTypeConfig> flagCfg = new ArrayList<>();

    private FlagDataTypeConfig defaultCfg;
    // default to localhost
    private String hdfs = "hdfs://localhost:9000";

    private String datawaveHome;
    // default path
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private String baseHDFSDir = "/data/ShardIngest";
    private int socketPort;
    private String flagFileDirectory;

    public static final String DEFAULT_FILE_PATTERN = "2*/*/*/*";

    // a list of file patterns.
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    @XmlElement(name = "filePattern")
    private final List<String> filePatterns = new ArrayList<>();

    // default timeout
    private long timeoutMilliSecs = (5L * DateUtils.A_MINUTE);
    // default sleep between cycles
    private long sleepMilliSecs = 15000L;
    // existing flag file count threshold after which the timeoutMilliSecs is ignored. Default is -1 disabling this check.
    protected int flagCountThreshold = UNSET;
    // max flag file length
    private int maxFileLength = Integer.MAX_VALUE;
    // should we set the timestamp on flag files to the last timestamp of the file contained therein
    private boolean setFlagFileTimestamp = true;
    // use folder for file timestamp instead of the actual file timestamp
    private boolean useFolderTimestamp = false;
    // location of where to store FlagMaker metrics
    private String flagMetricsDirectory = "/data/BulkIngest/FlagMakerMetrics";
    // number of threads for the flag maker lookup
    protected int maxHdfsThreads = 25;
    // maximum cache of HDFS directory cache size
    protected int directoryCacheSize = 2000;
    // directory cache timeout. Default is 2 Hours
    protected long directoryCacheTimeout = (2 * 60 * 60 * 1000);
    // implementation of FlagMaker to run
    private String flagMakerClass = FlagMaker.class.getName();

    private String flagDistributorClass = SimpleFlagDistributor.class.getName();

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
        validate(); // validate uses baseHDFSDir to alter the folders within the datatype configss
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
     * @return a list of datatype configurations
     */
    @XmlTransient
    public List<FlagDataTypeConfig> getFlagConfigs() {
        return flagCfg;
    }

    /**
     * Allows the programmatic addition of <code>FlagConfig</code>s to this object.
     *
     * @param fc
     *            configuration for datatype
     */
    public void addFlagConfig(FlagDataTypeConfig fc) {
        if (fc != null) {
            flagCfg.add(fc);
        }
    }

    /**
     * Validate config and set up folders for each data type. Here we have a few rules:
     * <ul>
     * <li>if you provide no folders, then we will assume that the folder is the data type which will be appended to the base directory (e.g.
     * /data/ShardIngest).</li>
     * <li>Users can provide absolute file paths by leading with a slash ("/")</li>
     * </ul>
     *
     * Note: this modifies teh FlagMakerConfig in several ways
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

        for (FlagDataTypeConfig cfg : this.flagCfg) {
            if (cfg.getInputFormat() == null) {
                throw new IllegalArgumentException("Input Format Class must be specified for data type: " + cfg.getDataName());
            }
            if (cfg.getIngestPool() == null) {
                throw new IllegalArgumentException("Ingest Pool must be specified for data type: " + cfg.getDataName());
            }
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
            cfg.setBaseHdfsDir(baseHDFSDir); // forces the folders to get reconfigured
        }
    }

    @Override
    public String toString() {
        //@formatter:off
        String result =
            "hdfs: " + this.getHdfs() + "\n" +
            "datawaveHome: " + this.getDatawaveHome() + "\n" +
            "baseHDFSDir: " + this.getBaseHDFSDir() + "\n" +
            "socketPort: " + this.getSocketPort() + "\n" +
            "flagFileDirectory: " + this.getFlagFileDirectory() + "\n" +
            "filePatterns: " + this.getFilePatterns() + "\n" +
            "timeoutMilliSecs: " + this.getTimeoutMilliSecs() + "\n" +
            "sleepMilliSecs: " + this.getSleepMilliSecs() + "\n" +
            "flagCountThreshold: " + this.getFlagCountThreshold() + "\n" +
            "maxFileLength: " + this.getMaxFileLength() + "\n" +
            "isSetFlagFileTimestamp: " + this.isSetFlagFileTimestamp() + "\n" +
            "useFolderTimestamp: " + this.isUseFolderTimestamp() + "\n" +
            "flagMetricsDirectory: " + this.getFlagMetricsDirectory() + "\n" +
            "maxHdfsThreads: " + this.getMaxHdfsThreads() + "\n" +
            "directoryCacheSize: " + this.getDirectoryCacheSize() + "\n" +
            "directoryCacheTimeout: " + this.getDirectoryCacheTimeout() + "\n" +
            "flagMakerClass: " + this.getFlagMakerClass() + "\n" +
            "flagDistributorClass: " + this.getFlagDistributorClass() + "\n" +
            "defaultCfg: " + this.getDefaultCfg() + "\n" +
            "flagCfg: " + this.getFlagConfigs() + "\n";
        //@formatter:on
        return result;
    }
}
