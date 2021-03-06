package datawave.common.test.utils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.StringJoiner;
import java.util.stream.Collectors;
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
            Collection<Path> deletePaths = allTmpPaths
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            // @formatter:on
            
            for (Path tmpPath : deletePaths) {
                Files.delete(tmpPath);
            }
        }
    }
    
    /**
     * Create a temporary file at a certain path depth for test purposes.
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
    public static String createTemporaryFile(String baseDir, int depth, String filePrefix, String fileSuffix) throws IOException {
        StringJoiner pathMaker = new StringJoiner(FileSystems.getDefault().getSeparator()).add(baseDir);
        
        // @formatter:off
        IntStream.range(1, depth + 1)
                .mapToObj(String::valueOf)
                .forEach(pathMaker::add);
        // @formatter:on
        
        return createTemporaryFile(pathMaker.add(filePrefix + depth + "." + fileSuffix).toString());
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
    public static String createTemporaryFile(String tmpFile) throws IOException {
        Path filePath = Paths.get(tmpFile);
        createTemporaryDir(filePath.getParent().toFile().getAbsolutePath());
        return Files.createFile(filePath).toFile().getAbsolutePath();
    }
    
    /**
     * Create a temporary directory
     *
     * @param tmpDir
     *            The directory to create
     * @return The directory that was created.
     * @throws IOException
     *             An exception is thrown if an I/O issue occurs while deleting
     */
    public static String createTemporaryDir(String tmpDir) throws IOException {
        return Files.createDirectories(Paths.get(tmpDir)).toFile().getAbsolutePath();
    }
}
