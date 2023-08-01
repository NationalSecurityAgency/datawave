package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.junit.Test;

public abstract class BufferedFileBackedRewritableSortedSetTest<K,V> extends BufferedFileBackedSortedSetTest<Map.Entry<K,V>> {

    @Override
    public abstract RewritableSortedSet.RewriteStrategy<Map.Entry<K,V>> getRewriteStrategy();

    @Override
    public Map.Entry<K,V> createData(byte[] values) {
        byte[] vbuffer = new byte[values.length];
        Arrays.fill(vbuffer, (byte) (values[0] + 1));
        return new UnmodifiableMapEntry(createKey(values), createValue(vbuffer));
    }

    public abstract K createKey(byte[] values);

    public abstract V createValue(byte[] values);

    public abstract void testFullEquality(Map.Entry<K,V> expected, Map.Entry<K,V> value);

    @Test
    public void testRewrite() throws Exception {
        // create a new set of data, only half of which has greater Values
        Map.Entry<K,V>[] data2 = new Map.Entry[template.length * 2];
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            byte[] vbuffer = new byte[buffer.length];
            Arrays.fill(vbuffer, (byte) (template[i] + 1));
            data2[i] = new UnmodifiableMapEntry(createKey(buffer), createValue(vbuffer));
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            byte[] vbuffer = new byte[buffer.length];
            Arrays.fill(vbuffer, (byte) (template[i] - 1));
            Map.Entry<K,V> datum = new UnmodifiableMapEntry(createKey(buffer), createValue(vbuffer));
            data2[i + template.length] = datum;
        }

        set = new BufferedFileBackedSortedSet.Builder().withComparator(getComparator()).withRewriteStrategy(getRewriteStrategy()).withBufferPersistThreshold(5)
                        .withMaxOpenFiles(7).withNumRetries(2)
                        .withHandlerFactories(Collections.singletonList(new BufferedFileBackedSortedSet.SortedSetFileHandlerFactory() {
                            @Override
                            public FileSortedSet.SortedSetFileHandler createHandler() throws IOException {
                                SortedSetTempFileHandler fileHandler = new SortedSetTempFileHandler();
                                tempFileHandlers.add(fileHandler);
                                return fileHandler;
                            }

                            @Override
                            public boolean isValid() {
                                return true;
                            }
                        })).withSetFactory(getFactory()).build();

        // adding in the data set multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            addDataRandomly(set, data);
            addDataRandomly(set, data2);
        }

        // now test the contents
        int index = 0;
        for (Iterator<Map.Entry<K,V>> it = set.iterator(); it.hasNext();) {
            Map.Entry<K,V> value = it.next();
            int dataIndex = sortedOrder[index++];
            Map.Entry<K,V> expected = (dataIndex < template.length ? data2[dataIndex] : data[dataIndex]);
            testFullEquality(expected, value);
        }
    }
}
