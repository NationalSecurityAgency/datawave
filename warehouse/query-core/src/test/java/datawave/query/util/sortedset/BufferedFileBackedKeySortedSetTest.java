package datawave.query.util.sortedset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Comparator;

import org.apache.accumulo.core.data.Key;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BufferedFileBackedKeySortedSetTest extends BufferedFileBackedSortedSetTest<Key> {

    @Override
    public Key createData(byte[] values) {
        return new Key(values);
    }

    @Override
    public Comparator<Key> getComparator() {
        return null;
    }

    @Override
    public FileSortedSet.FileSortedSetFactory<Key> getFactory() {
        return new FileKeySortedSet.Factory();
    }

}
