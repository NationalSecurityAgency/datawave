package nsa.datawave.query.util;

import com.google.common.collect.Maps;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class TypeMetadataBridge {
    
    private static final Logger log = Logger.getLogger(TypeMetadataBridge.class);
    private static DefaultFileSystemManager fileSystemManager = null;
    
    protected String uri;
    protected String dir;
    protected String fileName;
    protected String tempFileName;
    protected FileSystem fileSystem;
    protected Path tempPath;
    protected Path filePath;
    protected String[] metadataTableNames;
    
    protected Map<String,FileObject> fileObjectMap = Maps.newHashMap();
    
    public String getUri() {
        return uri;
    }
    
    public void setUri(String uri) throws Exception {
        
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
        return metadataTableNames;
    }
    
    public void setMetadataTableNames(String[] metadataTableNames) {
        this.metadataTableNames = metadataTableNames;
    }
    
    public String getTempFileName() {
        return this.tempFileName;
    }
    
    public FileObject getFileObject(String key) {
        return fileObjectMap.get(key);
    }
    
    // public void setFileObject(FileObject fileObject) {
    // this.fileObject = fileObject;
    // }
    
    public DefaultFileSystemManager getFileSystem() {
        return fileSystemManager;
    }
    
    public static void setFileSystemManager(DefaultFileSystemManager fileSystemManager) {
        TypeMetadataBridge.fileSystemManager = fileSystemManager;
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownThread()));
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
        log.debug("renamed " + source + " to " + sink);
    }
    
    public static class ShutdownThread implements Runnable {
        @Override
        public void run() {
            try {
                fileSystemManager.close();
            } catch (Exception e) {
                // do nothing, we are shutting down anyway
            }
        }
    }
    
    public void init() throws Exception {
        for (String metadataTableName : this.metadataTableNames) {
            FileObject fileObject = fileSystemManager.resolveFile(this.getUri() + this.getDir() + "/" + metadataTableName + "/" + this.getFileName());
            this.fileObjectMap.put(metadataTableName, fileObject);
        }
        // this.fileObject = fileSystemManager.resolveFile(this.getUri() + this.getDir() + "/" + this.getFileName());
    }
    
    @Override
    public String toString() {
        return "TypeMetadataBridge{" + "dir='" + dir + '\'' + ", uri='" + uri + '\'' + ", fileName='" + fileName + '\'' + ", tempFileName='" + tempFileName
                        + '\'' + ", fileSystem=" + fileSystem + ", tempPath=" + tempPath + ", filePath=" + filePath + ", fileObjectMap=" + fileObjectMap + '}';
    }
}
