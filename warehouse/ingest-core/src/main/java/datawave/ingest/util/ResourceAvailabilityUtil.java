package datawave.ingest.util;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Utility for checking resource availability, such as disk space and memory.
 */
public class ResourceAvailabilityUtil {

    private static Set<String> LOGGED_EXCEPTIONS = new HashSet<>(0);
    public static final String ROOT_PATH = "/";

    /**
     * Tests whether available JVM memory is equal to or greater than the specified threshold
     *
     * @param minPercentageAvailable
     *            Minimum amount of available memory, as a percentage of total memory
     * @return True, if available memory is equal to or greater than the specified threshold
     */
    public static boolean isMemoryAvailable(float minPercentageAvailable) {
        final Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();

        return ((((double) freeMemory / (double) totalMemory))) >= minPercentageAvailable;
    }

    /**
     * Tests whether available disk space is equal to or greater than the specified threshold. The default path is the current directory in which the JVM is
     * operating.
     *
     * @param minPercentageAvailable
     *            Minimum amount of available disk space, as a percentage of total memory
     * @return True, if available disk space is equal to or greater than the specified threshold
     */
    public static boolean isDiskAvailable(float minPercentageAvailable) {
        return isDiskAvailable(ROOT_PATH, minPercentageAvailable);
    }

    /**
     * Tests whether available disk space is equal to or greater than the specified threshold
     *
     * @param path
     *            A file system path to be tested for available space
     * @param minPercentageAvailable
     *            Minimum amount of available disk space, as a percentage of total memory
     * @return True, if available disk space is equal to or greater than the specified threshold
     */
    public static boolean isDiskAvailable(final String path, float minPercentageAvailable) {
        try {
            final File file;
            if (null != path) {
                file = new File(path);
            } else {
                file = new File(ROOT_PATH);
            }

            long totalSpace = file.getTotalSpace();
            long usableSpace = file.getUsableSpace();

            return (((double) usableSpace / (double) totalSpace)) >= minPercentageAvailable;
        } catch (final Throwable e) {
            final String toString = e.toString();
            if (!LOGGED_EXCEPTIONS.contains(toString)) {
                Logger.getLogger(ResourceAvailabilityUtil.class).warn("Unable to check disk space availability based on path " + path, e);
                LOGGED_EXCEPTIONS.add(toString);
            }
        }

        return true;
    }
}
