package nsa.datawave.query.iterators;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@Deprecated
public interface JumpSeek<K extends WritableComparable<?>> {
    /**
     * This method is meant to be equivalent to the seek method on a SortedKeyValueIterator, except that it will only actually seek if the jumpKey is past the
     * current position of this iterator.
     *
     * @param jumpKey
     * @return true if hasTop() upon completion
     * @throws IOException
     */
    public boolean jump(K jumpKey) throws IOException;
}
