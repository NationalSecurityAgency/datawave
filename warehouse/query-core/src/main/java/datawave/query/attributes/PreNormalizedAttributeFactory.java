package datawave.query.attributes;

import datawave.typemetadata.TypeMetadata;

import org.apache.accumulo.core.data.Key;

public class PreNormalizedAttributeFactory extends AttributeFactory {
    
    public PreNormalizedAttributeFactory(TypeMetadata typeMetadata) {
        super(typeMetadata);
    }
    
    @Override
    public Attribute<?> create(String fieldName, String data, Key key, boolean toKeep) {
        return new PreNormalizedAttribute(data, key, toKeep);
    }
}
