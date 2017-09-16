package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class StringType extends BaseType<String> {
    
    private static final long serialVersionUID = 8143572646109171126L;
    
    public StringType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
}
