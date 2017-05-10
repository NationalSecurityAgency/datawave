package datawave.query.rewrite.attributes;

import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;

public class PreNormalizedAttributeFactory extends AttributeFactory {
    
    public PreNormalizedAttributeFactory(TypeMetadata typeMetadata) {
        super(typeMetadata);
    }
    
    @Override
    public Attribute<?> create(String fieldName, String data, Key key, boolean toKeep) {
        return new PreNormalizedAttribute(data, key, toKeep);
    }
}
