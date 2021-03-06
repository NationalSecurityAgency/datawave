package datawave.common.test.utils;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.StringJoiner;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FileUtils {
    
    /**
     * Delete all files under a directory.
     *
     * @param filePath
     *            Local directory to delete
     * @throws IOException
     *             An exception is thrown if an I/O issue occurs while deleting
     */
    public static void deletePath(String filePath) throws IOException {
        try (Stream<Path> allTmpPaths = java.nio.file.Files.walk(Paths.get(filePath), Integer.MAX_VALUE, FileVisitOption.FOLLOW_LINKS)) {
            
            // @formatter:off
            boolean allDeleted = allTmpPaths
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .allMatch(File::delete);
            // @formatter:on
            
            if (!allDeleted) {
                throw new IOException("Unable to delete all artifacts in " + filePath);
            }
        }
    }
    
    /**
     * Create a temporary file for test purposes.
     *
     * @param baseDir
     *            The top level parent directory where the files will be created
     * @param depth
     *            The depth from the top level parent where the file will be created
     * @param filePrefix
     *            The file name before the depth level is appended
     * @param fileSuffix
     *            The file extension
     * @throws IOException
     *             An exception is thrown if an I/O issue occurs while creating file.
     */
    public static File createTemporaryFile(String baseDir, int depth, String filePrefix, String fileSuffix) throws IOException {
        StringJoiner pathMaker = new StringJoiner(File.separator).add(baseDir);
        
        // @formatter:off
        IntStream.range(1, depth + 1)
                .mapToObj(String::valueOf)
                .sorted()
                .forEach(pathMaker::add);
        // @formatter:on
        
        return createTemporaryFile(new File(pathMaker.add(filePrefix + depth + "." + fileSuffix).toString()));
    }
    
    /**
     * Create a temporary file
     *
     * @param tmpFile
     *            The file to create
     * @return The file that was created.
     * @throws IOException
     *             An exception is thrown if an I/O issue occurs while deleting
     */
    public static File createTemporaryFile(File tmpFile) throws IOException {
        Files.createParentDirs(tmpFile);
        
        if (!tmpFile.createNewFile()) {
            throw new IOException("Unable to create " + tmpFile);
        }
        return tmpFile;
    }
}
