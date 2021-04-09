package datawave.common.io;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.http.util.Args;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesFinder {
    
    /**
     * Takes a classpath's value and resolves relative paths and returns a list of paths
     *
     * @param classpath
     *            Classpath value to read to gather file paths
     * @param delim
     *            Delimiter to use to split environment variable value.
     * @return A collection of files
     */
    public static Collection<String> getFilesFromClasspath(String classpath, String delim) {
        // @formatter:off
        return Arrays
                .stream(classpath.split(delim))
                .filter(path -> !path.isEmpty())
                .map(FilesFinder::convertToCanonicalPath)
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    /**
     * Traverses a directory path for a files that match a pattern up to a max depth and returns a list of paths
     *
     * @param path
     *            Path to search
     * @param filePattern
     *            Pattern to match
     * @param maxDepth
     *            Maximum file tree depth to search
     * @return A collection of files
     * @throws IOException
     *             If an error occurs while traversing the input directory
     */
    public static Collection<String> getFilesFromPattern(String path, String filePattern, int maxDepth) throws IOException {
        Path dirPath = Paths.get(Args.notBlank(path, "Path must not be null or blank"));
        
        if (!java.nio.file.Files.isDirectory(dirPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Input path " + path + " must be a directory");
        }
        
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + filePattern);
        try (Stream<Path> filesWalked = java.nio.file.Files.walk(dirPath, maxDepth, FileVisitOption.FOLLOW_LINKS)) {
            // @formatter:off
            return filesWalked
                    .filter(java.nio.file.Files::isReadable)
                    .filter(java.nio.file.Files::isRegularFile)
                    .filter(matcher::matches)
                    .map(Path::toString)
                    .collect(Collectors.toList());
            // @formatter:on
        }
    }
    
    /**
     * Verify absolute path
     *
     * @param pathEntry
     *            Path entry
     * @return Resolved path entry
     */
    private static String convertToCanonicalPath(String pathEntry) {
        File filePath = new File(pathEntry);
        
        if (!filePath.exists()) {
            throw new IllegalArgumentException(pathEntry + " does not exist on file system.");
        }
        
        try {
            return filePath.getCanonicalPath();
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert " + pathEntry + " to a canonical path.", e);
        }
    }
    
    /**
     * Will search the source path for files that match the pattern
     *
     * @param fileSystem
     *            The file system
     * @param sourcePath
     *            The source path to list
     * @param pattern
     *            The pattern to evaluate matching files
     * @return An array of files status objects representing matching files.
     */
    public static FileStatus[] listPath(FileSystem fileSystem, org.apache.hadoop.fs.Path sourcePath, Pattern pattern) {
        try {
            return fileSystem.listStatus(sourcePath, getPathNameFilter(pattern));
        } catch (IOException e) {
            throw new RuntimeException("Unable to list files from " + sourcePath + " of pattern" + pattern, e);
        }
    }
    
    /**
     * Will create a PathFilter based on a pattern
     *
     * @param pattern
     *            A pattern to filter matches
     * @return A path filter that will evaluate files based on a pattern
     */
    private static PathFilter getPathNameFilter(Pattern pattern) {
        return path -> pattern.matcher(path.getName()).matches();
    }
    
}
