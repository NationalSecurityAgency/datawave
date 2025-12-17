package datawave.query.tables.keyword.transform;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.util.keyword.TagCloudPartition;

public class TagCloudPartitionTransformer implements TagCloudInputTransformer<TagCloudPartition> {
    private final static TagCloudPartitionTransformer instance = new TagCloudPartitionTransformer();

    /**
     * Has no state and is thread safe, use a singleton
     *
     * @return
     */
    public static TagCloudPartitionTransformer getInstance() {
        return instance;
    }

    private TagCloudPartitionTransformer() {}

    @Override
    public Entry<Key,Value> encode(TagCloudPartition partition) {
        return Map.entry(new Key(), new Value(TagCloudPartition.serialize(partition)));
    }

    @Override
    public boolean canDecode(Entry<Key,Value> input) {
        try {
            return TagCloudPartition.canDeserialize(input.getValue().get());
        } catch (IOException e) {
            throw new RuntimeException("error checking decoding " + input.getKey(), e);
        }
    }

    @Override
    public TagCloudPartition decode(Entry<Key,Value> input) {
        try {
            return TagCloudPartition.deserialize(input.getValue().get());
        } catch (IOException e) {
            throw new RuntimeException("Could not deserialize KeywordResults from K/V " + input.getKey(), e);
        }
    }
}
