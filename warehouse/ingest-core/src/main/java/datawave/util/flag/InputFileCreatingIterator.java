package datawave.util.flag;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps an Iterator of LocatedFileStatus items, converting them into InputFile objects
 */
public class InputFileCreatingIterator implements Iterator<InputFile> {
    private static final Logger log = LoggerFactory.getLogger(InputFileCreatingIterator.class);

    private final Iterator<LocatedFileStatus> delegate;
    private final String inputFolder;
    private final String baseDir;
    private final boolean useFolderTimestamp;

    public InputFileCreatingIterator(Iterator<LocatedFileStatus> delegate, String inputFolder, String baseDir, boolean useFolderTimestamp) {
        this.delegate = delegate;
        this.inputFolder = inputFolder;
        this.baseDir = baseDir;
        this.useFolderTimestamp = useFolderTimestamp;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public InputFile next() {
        if (!delegate.hasNext()) {
            throw new NoSuchElementException("Delegate hasNext returned false");
        }

        LocatedFileStatus status = delegate.next();

        if (null == status) {
            throw new NoSuchElementException("Received null object from delegate");
        }

        if (log.isTraceEnabled()) {
            log.trace("Adding file {}", status.getPath());
            log.trace("File {} : {}", inputFolder, status);
        }
        return new InputFile(inputFolder, status, this.baseDir, this.useFolderTimestamp);
    }
}
