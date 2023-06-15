package datawave.common.io;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/** Generally useful File utilities. */
public class Files {
    private static final String NULL_PARAMS = Files.class.getName() + " does not accept null parameters.";
    private static final String DIR_NO_EXIST = "Directory, '%s' does not exist; and cannot be created";
    private static final String FILE_NOT_DIR = "File, '%s' is not a directory.";
    private static final String DIR_NOT_READABLE = "Directory, '%s' cannot be read.";
    private static final String DIR_NO_EXEC = "Directory contents for '%s' cannot be listed.";
    private static final String DIR_NOT_WRITABLE = "Directory, '%s' cannot be written to.";

    private static final String FILE_NOT_READABLE = "File, '%s' cannot be read.";
    private static final String FILE_NO_EXIST = "File, '%s' does not exist.";
    private static final String DEST_EXISTS = "File, '%s' already exists.";
    private static final String DIR_NOT_FILE = "Directory, '%s' is not a file.";
    private static final String CANT_OVERWRITE = "Destination, '%s' cannot be overwritten.";
    private static final String FILE_NOT_WRITABLE = "File, '%s' cannot be written to.";
    private static final String RENAME_ERROR = "Error moving '%s' -> '%s' - %s";

    private Files() {
        throw new IllegalStateException("Do not instantiate utility class.");
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, and canExecute.
     *
     * @param dir
     *            The directory to check.
     * @throws IllegalStateException
     *             if the given {@link File} does not pass.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static void ensureDir(final String dir) {
        ensureDir(dir, false);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @param checkWrite
     *            Whether or not to check if the directory is writable.
     * @throws IllegalStateException
     *             if the given {@link File} does not pass.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static void ensureDir(final String dir, final boolean checkWrite) {
        final String err = checkDir(dir, checkWrite);
        if (err != null)
            throw new IllegalStateException(err);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @throws IllegalStateException
     *             if the given {@link File} does not pass.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static void ensureDir(final File dir) {
        ensureDir(dir, false);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @param checkWrite
     *            Whether or not to check if the directory is writable.
     * @throws IllegalStateException
     *             if the given {@link File} does not pass.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static void ensureDir(final File dir, final boolean checkWrite) {
        final String err = checkDir(dir, checkWrite);
        if (err != null)
            throw new IllegalStateException(err);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, and canExecute.
     *
     * @param dir
     *            The directory to check.
     * @return an error message if there are any problems, null otherwise.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String checkDir(final String dir) {
        return checkDir(dir, false);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @param checkWrite
     *            Whether or not to check if the directory is writable.
     *
     * @return an error message if there are any problems, null otherwise.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String checkDir(final String dir, final boolean checkWrite) {
        if (dir == null)
            throw new IllegalArgumentException(NULL_PARAMS);
        return checkDir(new File(dir), checkWrite);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @return an error message if there are any problems, null otherwise.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String checkDir(final File dir) {
        return checkDir(dir, false);
    }

    /**
     * Performs a variety of checks to ensure that the directory is valid. Checks include: exists||mkdirs, isDirectory, canRead, canExecute, and (optionally)
     * canWrite.
     *
     * @param dir
     *            The directory to check.
     * @param checkWrite
     *            Whether or not to check if the directory is writable.
     *
     * @return an error message if there are any problems, null otherwise.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String checkDir(final File dir, final boolean checkWrite) {
        if (dir == null)
            throw new IllegalArgumentException(NULL_PARAMS);

        if (!(dir.exists() || dir.mkdirs()))
            return String.format(DIR_NO_EXIST, dir);
        if (!dir.isDirectory())
            return String.format(FILE_NOT_DIR, dir);
        if (!dir.canRead())
            return String.format(DIR_NOT_READABLE, dir);
        if (!dir.canExecute())
            return String.format(DIR_NO_EXEC, dir);
        if (checkWrite && !dir.canWrite())
            return String.format(DIR_NOT_WRITABLE, dir);

        return null;
    }

    /**
     * Performs a variety of checks to ensure that the file is valid. Checks include: exists, isFile, canRead, and (optionally) canWrite.
     *
     * @param file
     *            The file to check
     * @param checkWrite
     *            Whether or not to check writable.
     * @return an error message if there are any problems, null otherwise.
     */
    public static String checkFile(final File file, final boolean checkWrite) {
        if (file == null)
            throw new IllegalArgumentException(NULL_PARAMS);

        if (!file.exists())
            return String.format(FILE_NO_EXIST, file);
        if (file.isDirectory())
            return String.format(DIR_NOT_FILE, file);
        if (!file.canRead())
            return String.format(FILE_NOT_READABLE, file);
        if (checkWrite && !file.canWrite())
            return String.format(FILE_NOT_WRITABLE, file);

        return null;
    }

    /**
     * Attempts to move the source file to the destination file. Normalizes the platform-dependent nature of {@link File#renameTo(File)} while providing more
     * detailed error messages.
     *
     * Overwrite defaults to false.
     *
     * @param src
     *            The name of the File to be moved
     * @param dest
     *            The name of the destination File.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     * @throws IOException
     *             if there are any problems moving the file.
     */
    public static void ensureMv(final String src, final String dest) throws IOException {
        ensureMv(src, dest, false);
    }

    /**
     * Attempts to move the source file to the destination file, optionally overwriting the destination file if it exists. Normalizes the platform-dependent
     * nature of {@link File#renameTo(File)} while providing more detailed error messages.
     *
     * @param src
     *            The name of the File to be moved
     * @param dest
     *            The name of the destination File.
     * @param overwrite
     *            Whether or not to overwrite an existing destination file.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     * @throws IOException
     *             if there are any problems moving the file.
     */
    public static void ensureMv(final String src, final String dest, final boolean overwrite) throws IOException {
        if (src == null || dest == null)
            throw new IllegalArgumentException(NULL_PARAMS);
        ensureMv(new File(src), new File(dest), overwrite);
    }

    /**
     * Attempts to move the source file to the destination file. Normalizes the platform-dependent nature of {@link File#renameTo(File)} while providing more
     * detailed error messages.
     *
     * Overwrite defaults to false.
     *
     * @param src
     *            The file to be moved
     * @param dest
     *            The move destination.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     * @throws IOException
     *             if there are any problems moving the file.
     */
    public static void ensureMv(final File src, final File dest) throws IOException {
        ensureMv(src, dest, false);
    }

    /**
     * Attempts to move the source file to the destination file, optionally overwriting the destination file if it exists. Normalizes the platform-dependent
     * nature of {@link File#renameTo(File)} while providing more detailed error messages.
     *
     * @param src
     *            The file to be moved
     * @param dest
     *            The move destination.
     * @param overwrite
     *            Whether or not to overwrite an existing destination file.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     * @throws IOException
     *             if there are any problems moving the file.
     */
    public static void ensureMv(final File src, final File dest, final boolean overwrite) throws IOException {
        final String err = mv(src, dest, overwrite);
        if (err != null)
            throw new IOException(err);
    }

    /**
     * Attempts to move the source file to the destination file. Normalizes the platform-dependent nature of {@link File#renameTo(File)} while providing more
     * detailed error messages.
     *
     * Overwrite defaults to false.
     *
     * @param src
     *            The name of the file to be moved.
     * @param dest
     *            The name of the new destination.
     * @return an error message if there are any problems.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String mv(final String src, final String dest) {
        if (src == null || dest == null)
            throw new IllegalArgumentException(NULL_PARAMS);
        return mv(new File(src), new File(dest));
    }

    /**
     * Attempts to move the source file to the destination file, optionally overwriting the destination file if it exists. Normalizes the platform-dependent
     * nature of {@link File#renameTo(File)} while providing more detailed error messages.
     *
     * @param src
     *            The name of the file to be moved.
     * @param dest
     *            The name of the new destination.
     * @param overwrite
     *            Whether to overwrite an existing desination file.
     * @return an error message if there are any problems.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String mv(final String src, final String dest, final boolean overwrite) {
        if (src == null || dest == null)
            throw new IllegalArgumentException(NULL_PARAMS);
        return mv(new File(src), new File(dest), overwrite);
    }

    /**
     * Attempts to move the source file to the destination file, optionally overwriting the destination file if it exists. Normalizes the platform-dependent
     * nature of {@link File#renameTo(File)} while providing more detailed error messages.
     *
     * @param src
     *            The file to be moved.
     * @param dest
     *            The new destination of the file.
     * @return an error message if there are any problems.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String mv(final File src, final File dest) {
        return mv(src, dest, false);
    }

    /**
     * Attempts to move the source file to the destination file, optionally overwriting the destination file if it exists. Normalizes the platform-dependent
     * nature of {@link File#renameTo(File)} while providing more detailed error messages.
     *
     * @param src
     *            The file to be moved.
     * @param dest
     *            The new destination.
     * @param overwrite
     *            Whether to overwrite an existing desination file.
     * @return an error message if there are any problems.
     * @throws IllegalArgumentException
     *             if any of the parameters are null.
     */
    public static String mv(final File src, final File dest, final boolean overwrite) {
        if (src == null || dest == null)
            throw new IllegalArgumentException(NULL_PARAMS);

        String msg = checkFile(src, false);
        if (msg != null)
            return msg;

        if (dest.exists()) {
            if (!overwrite)
                return String.format(DEST_EXISTS, dest);

            msg = checkFile(dest, true);
            if (msg != null)
                return msg;

            if (!dest.delete())
                return String.format(CANT_OVERWRITE, dest);
        } else {
            msg = checkDir(dest.getParentFile(), true);
            if (msg != null)
                return msg;
        }

        try {
            // Works even when moving between File Systems.
            FileUtils.moveFile(src, dest);
        } catch (IOException ioEx) {
            return String.format(RENAME_ERROR, src, dest, ioEx.getMessage());
        }

        return null;
    }

    /** @return the system's "temp" directory */
    public static File tmpDir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }
}
