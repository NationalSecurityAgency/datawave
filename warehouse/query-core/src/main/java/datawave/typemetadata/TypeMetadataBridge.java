package datawave.typemetadata;

import com.google.common.collect.Maps;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class TypeMetadataBridge {
    
    public static class Builder<B extends Builder<B>> {
        private String uri;
        private String dir;
        private String fileName;
        private String tempFileName;
        private FileSystem fileSystem;
        private FileSystemManager fileSystemManager;
        private Path tempPath;
        private Path filePath;
        private String[] tableNames;
        
        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }
        
        public B withUri(String uri) {
            this.uri = uri;
            return self();
        }
        
        public B withDir(String dir) {
            this.dir = dir;
            return self();
        }
        
        public B withFileName(String fileName) {
            this.fileName = fileName;
            return self();
        }
        
        public B withFileSystem(FileSystem fileSystem) {
            this.fileSystem = fileSystem;
            return self();
        }
        
        public B withTempPath(Path tempPath) {
            this.tempPath = tempPath;
            return self();
        }
        
        public B withFilePath(Path filePath) {
            this.filePath = filePath;
            return self();
        }
        
        public B withTableNames(String[] tableNames) {
            this.tableNames = tableNames;
            return self();
        }
        
        public B withFileSystemManager(FileSystemManager fileSystemManager) {
            this.fileSystemManager = fileSystemManager;
            return self();
        }
        
        public TypeMetadataBridge build() {
            return new TypeMetadataBridge(this);
        }
    }
    
    public static Builder<?> builder() {
        return new Builder();
    }
    
    private static final Logger log = Logger.getLogger(TypeMetadataBridge.class);
    private FileSystemManager fileSystemManager;
    
    protected String uri;
    protected String dir;
    protected String fileName;
    protected String tempFileName;
    protected FileSystem fileSystem;
    protected Path tempPath;
    protected Path filePath;
    protected String[] tableNames;
    
    protected Map<String,FileObject> fileObjectMap = Maps.newHashMap();
    
    private TypeMetadataBridge() {}
    
    private TypeMetadataBridge(Builder<?> builder) {
        this(builder.uri, builder.dir, builder.fileName, builder.tempFileName, builder.fileSystem, builder.fileSystemManager, builder.tempPath,
                        builder.filePath, builder.tableNames);
    }
    
    private TypeMetadataBridge(String uri, String dir, String fileName, String tempFileName, FileSystem fileSystem, FileSystemManager fileSystemManager,
                    Path tempPath, Path filePath, String[] tableNames) {
        this.uri = uri;
        this.dir = dir;
        this.fileName = fileName;
        this.tempFileName = tempFileName;
        this.fileSystem = fileSystem;
        this.fileSystemManager = fileSystemManager;
        this.tempPath = tempPath;
        this.filePath = filePath;
        this.tableNames = tableNames;
    }
    
    public String getUri() {
        return uri;
    }
    
    public void setUri(String uri) throws Exception {
        log.debug("I just set the uri to:" + uri);
        this.uri = uri;
        this.fileSystem = FileSystem.get(new URI(this.uri), new Configuration());
    }
    
    public String getDir() {
        return dir;
    }
    
    public void setDir(String dir) {
        this.dir = dir;
    }
    
    public String getFileName() {
        return fileName;
    }
    
    public void setFileName(String fileName) {
        
        this.fileName = fileName;
        this.tempFileName = "." + fileName;
    }
    
    public String[] getMetadataTableNames() {
        return tableNames;
    }
    
    public void setMetadataTableNames(String[] tableNames) {
        this.tableNames = tableNames;
    }
    
    public String getTempFileName() {
        return this.tempFileName;
    }
    
    public FileObject getFileObject(String key) {
        return fileObjectMap.get(key);
    }
    
    public FileSystemManager getFileSystemManager() {
        return fileSystemManager;
    }
    
    /**
     *
     * @param metadataTableName
     *            - the name of the table that the TYpeMetadata came from
     * @return
     * @throws Exception
     */
    public FSDataOutputStream getOutputStream(String metadataTableName) throws Exception {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new URI(this.uri), new Configuration());
        }
        return fileSystem.create(new Path(this.dir + "/" + metadataTableName + "/" + this.tempFileName));
    }
    
    public void rename(String metadataTableName) throws IOException {
        Path source = new Path(this.dir + "/" + metadataTableName + "/" + this.tempFileName);
        Path sink = new Path(this.dir + "/" + metadataTableName + "/" + this.fileName);
        this.fileSystem.delete(sink, false);
        this.fileSystem.rename(source, sink);
        log.debug("table:" + metadataTableName + " renamed " + source + " to " + sink);
    }
    
    public TypeMetadataBridge init() throws Exception {
        for (String tableName : this.tableNames) {
            FileObject fileObject = fileSystemManager.resolveFile(this.getUri() + this.getDir() + "/" + tableName + "/" + this.getFileName());
            this.fileObjectMap.put(tableName, fileObject);
        }
        return this;
    }
    
    @Override
    public String toString() {
        return "TypeMetadataBridge{" + "dir='" + dir + '\'' + ", uri='" + uri + '\'' + ", fileName='" + fileName + '\'' + ", tempFileName='" + tempFileName
                        + '\'' + ", fileSystem=" + fileSystem + ", tempPath=" + tempPath + ", filePath=" + filePath + ", fileObjectMap=" + fileObjectMap + '}';
    }
}
