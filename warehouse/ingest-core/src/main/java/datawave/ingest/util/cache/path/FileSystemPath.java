package datawave.ingest.util.cache.path;

import datawave.common.io.HadoopFileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/** Class that will map an output path to its file system specified by the passed in configurations */
public class FileSystemPath implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemPath.class);
    
    private final Path outputPath;
    private final FileSystem fileSystem;
    
    public FileSystemPath(Path outputPath, Collection<Configuration> confs) {
        Optional<FileSystem> optFs = HadoopFileSystemUtils.getFileSystem(confs, outputPath);
        if (!optFs.isPresent()) {
            throw new IllegalArgumentException("Unable to create filesystem for " + outputPath + " with " + confs);
        }
        
        this.fileSystem = optFs.get();
        this.outputPath = outputPath;
    }
    
    public FileSystem getFileSystem() {
        return fileSystem;
    }
    
    public Path getOutputPath() {
        return outputPath;
    }
    
    @Override
    public void close() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            LOGGER.warn("Unable to close {}", fileSystem, e);
        }
    }
}
