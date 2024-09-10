package datawave.query.tld;

import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

public class DedupeColumnFamilies implements Predicate<Entry<Key,Value>> {
    private final HashSet<ByteSequence> elements;

    public DedupeColumnFamilies() {
        elements = Sets.newHashSetWithExpectedSize(1024);
    }

    @Override
    public boolean apply(Entry<Key,Value> input) {
        ByteSequence cf = input.getKey().getColumnFamilyData();
        return elements.add(cf);
    }
}
