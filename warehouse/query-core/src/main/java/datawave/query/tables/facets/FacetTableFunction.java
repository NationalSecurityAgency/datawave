package datawave.query.tables.facets;

import java.io.IOException;
import java.util.Map.Entry;

import datawave.query.attributes.Cardinality;
import datawave.query.attributes.FieldValueCardinality;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.util.StringUtils;

public class FacetTableFunction implements Function<Entry<Key,Value>,Entry<Key,Document>> {
    
    @Override
    public Entry<Key,Document> apply(Entry<Key,Value> input) {
        Key key = input.getKey();
        Document newDoc = new Document();
        try {
            
            String[] fields = StringUtils.split(key.getColumnFamily().toString(), "\u0000");
            String[] fieldValues = StringUtils.split(key.getRow().toString(), "\u0000");
            
            FieldValueCardinality fvc = new FieldValueCardinality(HyperLogLogPlus.Builder.build(input.getValue().get()));
            fvc.setFieldName(fields[1]);
            fvc.setContent(fieldValues[1]);
            
            Cardinality card = new Cardinality(fvc, key, false);
            
            newDoc.put(fields[1], card);
            
            return Maps.immutableEntry(key, newDoc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
