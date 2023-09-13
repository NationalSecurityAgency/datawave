package datawave.util.flag;

import datawave.util.flag.config.FlagMakerConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Provides static methods for common flag file-related unit tests: retrieving input files, flagging files, flagged files, and flag files themselves.
 *
 */
public class FlagFileTestHelper {
    public static Collection<InputFile> listSortedInputFiles(FlagMakerConfig fmc, FileSystem fs) {
        ArrayList<InputFile> result = new ArrayList<>();
        try {
            for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(fmc.getBaseHDFSDir()), true); it.hasNext();) {
                LocatedFileStatus status = it.next();
                if (status.isFile()) {
                    Path path = status.getPath();
                    InputFile inputFile = new InputFile(status.getPath().getParent().getName(), path, status.getBlockSize(), status.getLen(),
                                    status.getModificationTime(), fmc.getBaseHDFSDir());
                    result.add(inputFile);
                }
            }
            result.sort(InputFile.LIFO);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    static List<File> retrieveFilesInInputDirectory(FlagMakerConfig fmc) throws IOException {
        return Files.walk(Paths.get(fmc.getBaseHDFSDir())).filter(FlagFileTestHelper::isInInputDirectory).map(java.nio.file.Path::toFile).filter(File::isFile)
                        .collect(Collectors.toList());
    }

    public static Path getPathToAnyInputFile(FileSystem fs, FlagMakerConfig fmc) throws IOException {
        Path file = null;
        for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(fmc.getBaseHDFSDir()), true); it.hasNext();) {
            LocatedFileStatus status = it.next();
            if (status.isFile()) {
                file = status.getPath();
                break;
            }
        }
        return file;
    }

    static List<File> retrieveFlaggingFiles(FlagMakerConfig fmc) throws IOException {
        return listFilesRecursively("/flagging", fmc);
    }

    static List<File> retrieveFlaggedFiles(FlagMakerConfig fmc) throws IOException {
        return listFilesRecursively("/flagged", fmc);
    }

    static File[] retrieveMetricsFiles(FlagMakerConfig fmc) {
        return new File(fmc.getFlagMetricsDirectory()).listFiles();
    }

    public static File[] listFlagFiles(FlagMakerConfig fmc) {
        return new File(fmc.getFlagFileDirectory()).listFiles();
    }

    /**
     * Note: expects exactly one flag file was created. If this isn't desired, use listFlagFiles instead
     *
     * @param fmc
     *            configuration to use
     * @return single flag file in flag file directory
     */
    public static String getFlagFilePath(FlagMakerConfig fmc) {
        File[] flagFiles = listFlagFiles(fmc);
        assertNotNull(flagFiles);
        assertEquals("Expected to find one flag file", 1, flagFiles.length);
        File flag = flagFiles[0];
        return flag.getPath();
    }

    public static void createTrackedDirs(final FileSystem fs, final InputFile file) throws IOException {
        final Path[] dirs = {file.getFlagged(), file.getFlagging(), file.getLoaded()};
        for (final Path dir : dirs) {
            final Path p = dir.getParent();
            if (!fs.mkdirs(p)) {
                throw new IllegalStateException("unable to create tracked directory (" + dir.getParent() + ")");
            }
        }
    }

    private static List<File> listFilesRecursively(String expectedDirectory, FlagMakerConfig fmc) throws IOException {
        return Files.walk(Paths.get(fmc.getBaseHDFSDir() + expectedDirectory)).map(java.nio.file.Path::toFile).filter(File::isFile)
                        .collect(Collectors.toList());
    }

    private static boolean isInInputDirectory(java.nio.file.Path path) {
        String pathAsString = path.toString();
        return !pathAsString.contains("flagging") && !pathAsString.contains("flagged");
    }

    public static String logFiles(File[] files) {
        StringBuilder result = new StringBuilder();
        for (File file : files) {
            result.append(file).append('\n');
        }
        for (File file : files) {
            result.append(logFileContents(file));
        }
        return result.toString();
    }

    public static String logFileContents(File file) {
        try {
            return com.google.common.io.Files.toString(file, Charset.defaultCharset()) + "EOF";
        } catch (Exception e) {
            return "Failed to read contents of " + file.getName();
        }
    }
}
