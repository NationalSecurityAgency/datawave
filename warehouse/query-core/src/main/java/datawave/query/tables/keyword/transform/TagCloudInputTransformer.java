package datawave.query.tables.keyword.transform;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.util.keyword.TagCloudPartition;

public interface TagCloudInputTransformer<K> {
    Entry<Key,Value> encode(K input);

    boolean canDecode(Entry<Key,Value> input);

    TagCloudPartition decode(Entry<Key,Value> input);
}
