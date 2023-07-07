package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;

import datawave.query.util.TypeMetadata;

public class PreNormalizedAttributeFactory extends AttributeFactory {

    public PreNormalizedAttributeFactory(TypeMetadata typeMetadata) {
        super(typeMetadata);
    }

    @Override
    public Attribute<?> create(String fieldName, String data, Key key, boolean toKeep) {
        return new PreNormalizedAttribute(data, key, toKeep);
    }
}
