package datawave.query.tables.keyword.extractor;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.query.attributes.Attribute;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.util.keyword.TagCloudPartition;

public interface TagCloudInputExtractor {
    default String getDocId(Key source) {
        String row = source.getRow().toString();
        String cf = source.getColumnFamily().toString();
        int index = cf.indexOf("\0");
        Preconditions.checkArgument(-1 != index);

        String dataType = cf.substring(0, index);
        String uid = cf.substring(index + 1);

        return row + "/" + dataType + "/" + uid;
    }

    default void initialize(Query settings) {}

    String getName();

    void extract(Key source, Map<String,Attribute<? extends Comparable<?>>> documentData) throws TagCloudInputExtractorException;

    TagCloudPartition get();

    void clear();

    TagCloudInputTransformer<TagCloudPartition> getInputTransformer();
}
