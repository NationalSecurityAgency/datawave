package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import datawave.util.flag.config.FlagMakerConfig;

/**
 * Provides static methods for common flag file-related unit tests: retrieving input files, flagging files, flagged files, and flag files themselves.
 *
 * This is meant to contain read-only operations
 */
public class FlagFileTestInspector {
    public static List<InputFile> listSortedInputFiles(FlagMakerConfig flagMakerConfig, FileSystem fs) {
        ArrayList<InputFile> result = new ArrayList<>();
        try {
            for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(flagMakerConfig.getBaseHDFSDir()), true); it.hasNext();) {
                LocatedFileStatus status = it.next();
                if (status.isFile()) {
                    Path path = status.getPath();
                    InputFile inputFile = new InputFile(status.getPath().getParent().getName(), path, status.getBlockSize(), status.getLen(),
                                    status.getModificationTime(), flagMakerConfig.getBaseHDFSDir());
                    result.add(inputFile);
                }
            }
            result.sort(InputFile.LIFO);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    public static List<File> listFilesInInputDirectory(FlagMakerConfig flagMakerConfig) throws IOException {
        return Files.walk(Paths.get(flagMakerConfig.getBaseHDFSDir())).filter(FlagFileTestInspector::isInInputDirectory).map(java.nio.file.Path::toFile)
                        .filter(File::isFile).collect(Collectors.toList());
    }

    public static List<File> listFlaggingFiles(FlagMakerConfig flagMakerConfig) throws IOException {
        return listFilesRecursively("/flagging", flagMakerConfig);
    }

    public static List<File> listFlaggedFiles(FlagMakerConfig flagMakerConfig) throws IOException {
        return listFilesRecursively("/flagged", flagMakerConfig);
    }

    public static List<File> listFlagFiles(FlagMakerConfig flagMakerConfig) {
        return toList(new File(flagMakerConfig.getFlagFileDirectory()).listFiles());
    }

    public static List<File> retrieveMetricsFiles(FlagMakerConfig flagMakerConfig) {
        return toList(new File(flagMakerConfig.getFlagMetricsDirectory()).listFiles());
    }

    public static Path getPathToAnyInputFile(FileSystem fs, FlagMakerConfig flagMakerConfig) throws IOException {
        Path file = null;
        for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(flagMakerConfig.getBaseHDFSDir()), true); it.hasNext();) {
            LocatedFileStatus status = it.next();
            if (status.isFile()) {
                file = status.getPath();
                break;
            }
        }
        return file;
    }

    public static File getOnlyFlagFile(FlagMakerConfig flagMakerConfig) {
        List<File> flagFiles = FlagFileTestInspector.listFlagFiles(flagMakerConfig);
        assertNotNull(flagFiles);
        assertEquals(1, flagFiles.size());

        return flagFiles.get(0);
    }

    /**
     * Note: expects exactly one flag file was created. If this isn't desired, use listFlagFiles instead
     *
     * @param flagMakerConfig
     *            configuration to use
     * @return single flag file in flag file directory
     */
    public static String getPathStringForOnlyFlagFile(FlagMakerConfig flagMakerConfig) {
        List<File> flagFiles = listFlagFiles(flagMakerConfig);
        assertNotNull(flagFiles);
        assertEquals("Expected to find one flag file", 1, flagFiles.size());
        File flag = flagFiles.get(0);
        return flag.getPath();
    }

    public static String logFiles(List<File> files) {
        if (null == files) {
            return "(null)";
        }
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

    private static List<File> toList(File[] files) {
        if (null != files) {
            return Arrays.asList(files);
        } else {
            return Collections.emptyList();
        }
    }

    private static List<File> listFilesRecursively(String expectedDirectory, FlagMakerConfig flagMakerConfig) throws IOException {
        return Files.walk(Paths.get(flagMakerConfig.getBaseHDFSDir() + expectedDirectory)).map(java.nio.file.Path::toFile).filter(File::isFile)
                        .collect(Collectors.toList());
    }

    private static boolean isInInputDirectory(java.nio.file.Path path) {
        String pathAsString = path.toString();
        return !pathAsString.contains("flagging") && !pathAsString.contains("flagged");
    }
}
