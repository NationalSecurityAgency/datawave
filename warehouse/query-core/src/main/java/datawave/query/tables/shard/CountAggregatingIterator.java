package datawave.query.tables.shard;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.core.iterators.ResultCountingIterator;
import datawave.marking.MarkingFunctions;

/**
 *
 */
public class CountAggregatingIterator extends TransformIterator {
    private static final Logger log = Logger.getLogger(CountAggregatingIterator.class);

    private Long count = 0l;
    private boolean firstTime = true;

    protected Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();

    private final MarkingFunctions markingFunctions;

    private Kryo kryo = new Kryo();

    public CountAggregatingIterator(Iterator<Entry<Key,Value>> iterator, Transformer transformer, MarkingFunctions markingFunctions) {
        super(iterator, transformer);
        this.markingFunctions = markingFunctions;
    }

    @Override
    public boolean hasNext() {
        if (count == -1) {
            return false;
        }

        boolean hasNext = false;
        if (getIterator().hasNext()) {
            hasNext = true;
            do {
                @SuppressWarnings("unchecked")
                Entry<Key,Value> entry = (Entry<Key,Value>) getIterator().next();

                if (null == entry || entry.getKey() == null || entry.getValue() == null) {
                    hasNext = false;
                    break;
                }

                // Unpack the kryo serialized object, it contains the count and the accumulated visibility
                ResultCountingIterator.ResultCountTuple tuple = unpackValue(entry.getValue());

                // Merge the columnVisibilities
                try {
                    this.columnVisibilities.add(tuple.getVisibility());
                } catch (Exception e) {
                    log.error("Error parsing columnVisibilities of key", e);
                    continue;
                }

                this.count += tuple.getCount();
            } while (getIterator().hasNext());
        } else if (firstTime) {
            firstTime = false;
            count = 0l;

            columnVisibilities.add(new ColumnVisibility(""));

            return true;
        }

        return hasNext;
    }

    private ResultCountingIterator.ResultCountTuple unpackValue(Value value) {
        ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
        Input input = new Input(bais);
        return kryo.readObject(input, ResultCountingIterator.ResultCountTuple.class);
    }

    @Override
    public Object next() {
        ColumnVisibility cv = null;

        try {
            // Calculate the columnVisibility for this key from the combination.
            cv = markingFunctions.combine(columnVisibilities);
        } catch (Exception e) {
            log.error("Could not create combined columnVisibilities for the count", e);
            return null;
        }

        Object obj = getTransformer().transform(Maps.immutableEntry(count, cv));
        count = -1l;
        return obj;
    }
}
