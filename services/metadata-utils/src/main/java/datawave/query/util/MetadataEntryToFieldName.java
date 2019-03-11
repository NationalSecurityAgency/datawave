package datawave.query.util;

import com.google.common.base.Function;

/**
 * 
 */
public class MetadataEntryToFieldName implements Function<MetadataEntry,String> {
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public String apply(MetadataEntry from) {
        return from.getFieldName();
    }
    
}
