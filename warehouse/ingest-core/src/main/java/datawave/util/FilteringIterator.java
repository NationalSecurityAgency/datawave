package datawave.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Wraps a RemoteIterator, returning only those FileStatus items that pass the provided PathFilter
 *
 * @param <T>
 */
public class FilteringIterator<T extends FileStatus> implements Iterator<T> {
    private final RemoteIterator<T> delegateIterator;
    private final PathFilter filter;
    private T matchingFileStatus = null;

    public FilteringIterator(RemoteIterator<T> sourceIterator, PathFilter filter) {
        this.delegateIterator = sourceIterator;
        this.filter = filter;
    }

    @Override
    public boolean hasNext() {
        findNextMatchingFile();

        return (this.matchingFileStatus != null);
    }

    @Override
    public T next() {
        this.findNextMatchingFile();

        T result = matchingFileStatus;
        matchingFileStatus = null;

        if (null == result) {
            throw new NoSuchElementException();
        }

        return result;
    }

    /**
     * Attempts to set this.matchingFileStatus by iterating through the delegate's FileStatus elements until one is accepted by the filter
     */
    private void findNextMatchingFile() {
        try {
            while (null == this.matchingFileStatus && this.delegateIterator.hasNext()) {
                T candidate = delegateIterator.next();
                if (candidate == null) {
                    this.matchingFileStatus = null;
                } else if (filter.accept(candidate.getPath())) {
                    this.matchingFileStatus = candidate;
                }
            }
        } catch (IOException e) {
            this.matchingFileStatus = null;
            throw new RuntimeException(e);
        }
    }
}
