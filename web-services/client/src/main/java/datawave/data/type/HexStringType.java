package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class HexStringType extends BaseType<String> {
    
    private static final long serialVersionUID = -3480716807342380164L;
    
    public HexStringType() {
        super(Normalizer.HEX_STRING_NORMALIZER);
    }
}
