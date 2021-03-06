package datawave.ingest.util.cache.path;

import datawave.common.io.HadoopFileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Class that will map an output path to its file system specified by the passed in configurations */
public class FileSystemPath implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemPath.class);
    
    private final Path outputPath;
    private final FileSystem fileSystem;
    
    public FileSystemPath(FileSystem fileSystem, Path outputPath) {
        this.fileSystem = fileSystem;
        this.outputPath = outputPath;
    }
    
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
            LOGGER.warn("Unable to close " + fileSystem, e);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        
        FileSystemPath that = ((FileSystemPath) obj);
        String thatOutputPath = that.getOutputPath().toUri().getPath();
        String thisOutputPath = outputPath.toUri().getPath();
        
        return thisOutputPath.equals(thatOutputPath) && this.getFileSystem().equals(that.fileSystem);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(outputPath.toUri().getPath());
    }
    
    @Override
    public String toString() {
        return "Path: " + outputPath.toUri().getPath();
    }
    
    /**
     * Will associate a file system to each input path
     *
     * @param paths
     *            Input paths to associate with a file system.
     * @param confs
     *            Hadoop configuration object used to find available file systems.
     * @return A collection of object that associate the path to its file system.
     */
    public static Collection<FileSystemPath> getFileSystemPaths(Collection<Path> paths, Collection<Configuration> confs) {
        // @formatter:off
        return paths
                .stream()
                .map(path -> new FileSystemPath(path, confs))
                .collect(Collectors.toList());
        // @formatter:on
    }
    
}
