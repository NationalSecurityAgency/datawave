package datawave.util.flag.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.mapreduce.InputFormat;

import datawave.ingest.input.reader.event.EventSequenceFileInputFormat;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@XmlRootElement
// allows (de)serialization of FlagDataTypeConfig alone
@XmlType
public class FlagDataTypeConfig {

    private String dataName;
    /*
     * List of folders to scan for input files. The folder will be considered an absolute path if it leads with a slash, otherwise relative to the FlagMaker's
     * base directory
     */
    @XmlTransient
    private List<String> folders;
    // todo - verify that "folders" isn't used any where in actual configuration files. If that's the case, then switch from folders to
    // list of relative paths
    // list of paths with base directory

    @XmlTransient
    @Setter(AccessLevel.NONE)
    private String baseHdfsDir;

    private String folder;

    private Class<? extends InputFormat> inputFormat = EventSequenceFileInputFormat.class;

    // number of reducers for datatype
    private int reducers;

    // maximum input files for datatype
    private int maxFlags;

    // which ingest pool to use for this datatype
    private String ingestPool;

    // extra arguments to pass to the ingest process
    private String extraIngestArgs;

    // arguments to be passed to the distributor
    private String distributionArgs = "none";

    // sort order for files within a flag file, based on the FileSystem's lastModified time for file
    private boolean lifo = false;

    // the FlagMaker's timeout for this datatype. A flag file must be created within this timeout if any data exists to be processed.
    private long timeoutMilliSecs = FlagMakerConfig.UNSET;

    // when the number of already-created flag files reaches this level, ignore timeoutMilliSecs Default is -1, disabling this override.
    private int flagCountThreshold = FlagMakerConfig.UNSET;

    // the ingest script to put in the flag file
    private String script;

    // if set, then the files will be placed after the fileListMarker in the flag file, one per line
    // @see IngestJob -inputFileLists and -inputFileListMarker flags.
    private String fileListMarker = null;

    private String collectMetrics;

    @Getter(AccessLevel.NONE)
    @XmlTransient
    private long last = System.currentTimeMillis();

    public FlagDataTypeConfig(String dataName, List<String> folder, int reducers, String extraIngestArgs) {
        this.dataName = dataName;
        this.folders = folder;
        this.reducers = reducers;
        this.extraIngestArgs = extraIngestArgs;
    }

    public FlagDataTypeConfig() {}

    @XmlTransient
    public long getLast() {
        return last;
    }

    /**
     * Deprecated. Use getFolders()
     *
     * @return folder
     */
    @Deprecated
    public String getFolder() {
        return folder;
    }

    /**
     * Deprecated. Use setFolders(List)
     */
    @Deprecated
    public void setFolder(String folder) {
        this.folder = folder;
        this.folders = null; // lazy initialize folders
    }

    public void setBaseHdfsDir(String baseHdfsDir) {
        this.baseHdfsDir = baseHdfsDir;
        if (null != this.baseHdfsDir) {
            this.baseHdfsDir = this.baseHdfsDir.trim();
            if (this.baseHdfsDir.charAt(this.baseHdfsDir.length() - 1) != '/') {
                this.baseHdfsDir = this.baseHdfsDir + "/";
            }
        }
        this.folders = null;
        initializeFolders();
    }

    @XmlTransient
    public List<String> getFolders() {
        initializeFolders();
        return folders;
    }

    public void setFolders(List<String> folders) {
        this.folders = folders;
        this.folder = String.join(",", folders);
    }

    private void initializeFolders() {
        if (folders == null || folders.isEmpty()) {
            this.folders = convertFolderIntoList();
        }

        // if nothing else, ensure the default path of "data name" is set
        if (folders.isEmpty() && null != this.dataName) {
            folders.add(this.dataName);
        }

        if (this.baseHdfsDir != null) {
            this.folders = prependBaseHdfsDirectoryToFolders();
        }

        if (folders.size() == 0) {
            folders = null;
        }
    }

    private ArrayList<String> convertFolderIntoList() {
        ArrayList<String> result = new ArrayList<>();
        if (folder != null && !folder.isEmpty()) {
            String[] tokens = folder.split(",");
            for (String token : tokens) {
                String trimmedToken = token.trim();
                if (trimmedToken.length() > 0) {
                    result.add(trimmedToken);
                }
            }
        }
        return result;
    }

    private List<String> prependBaseHdfsDirectoryToFolders() {
        List<String> prependedFolders = new ArrayList<>();
        for (String folder : folders) {
            if (!folder.startsWith("/") && !folder.startsWith(this.baseHdfsDir)) {
                prependedFolders.add(this.baseHdfsDir + folder);
            } else {
                prependedFolders.add(folder);
            }
        }
        return prependedFolders;
    }

    public String getCollectMetrics() {
        return collectMetrics;
    }

    public boolean isCollectMetrics() {
        return Boolean.parseBoolean(collectMetrics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlagDataTypeConfig that = (FlagDataTypeConfig) o;

        // this.last deliberately excluded
        //@formatter:off
        return reducers == that.reducers &&
                maxFlags == that.maxFlags &&
                lifo == that.lifo &&
                timeoutMilliSecs == that.timeoutMilliSecs &&
                flagCountThreshold == that.flagCountThreshold &&
                Objects.equals(dataName, that.dataName) &&
                Objects.equals(folders, that.folders) &&
                Objects.equals(inputFormat, that.inputFormat) &&
                Objects.equals(ingestPool, that.ingestPool) &&
                Objects.equals(extraIngestArgs, that.extraIngestArgs) &&
                Objects.equals(distributionArgs, that.distributionArgs) &&
                Objects.equals(script, that.script) &&
                Objects.equals(fileListMarker, that.fileListMarker) &&
                Objects.equals(collectMetrics, that.collectMetrics);
        //@formatter:on
    }

    @Override
    public int hashCode() {
        // this.last deliberately excluded
        return Objects.hash(dataName, folders, inputFormat, reducers, maxFlags, ingestPool, extraIngestArgs, distributionArgs, lifo, timeoutMilliSecs,
                        flagCountThreshold, script, fileListMarker, collectMetrics);
    }

    @Override
    public String toString() {
        //@formatter:off
        return "FlagDataTypeConfig{" +
                "dataName='" + dataName + '\'' +
                ", folders=" + getFolders() + // getFolders guarantees initialization of list from legacy "folder" tag
                ", inputFormat=" + inputFormat +
                ", reducers=" + reducers +
                ", maxFlags=" + maxFlags +
                ", ingestPool='" + ingestPool + '\'' +
                ", extraIngestArgs='" + extraIngestArgs + '\'' +
                ", distributionArgs='" + distributionArgs + '\'' +
                ", lifo=" + lifo + ", timeoutMilliSecs=" + timeoutMilliSecs +
                ", flagCountThreshold=" + flagCountThreshold +
                ", script='" + script + '\'' +
                ", fileListMarker='" + fileListMarker + '\'' +
                ", collectMetrics='" + collectMetrics + '\'' +
                ", last=" + last + '}';
        //@formatter:on
    }
}
