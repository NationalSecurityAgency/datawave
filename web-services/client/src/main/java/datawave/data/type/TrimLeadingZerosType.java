package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class TrimLeadingZerosType extends BaseType<String> {
    
    private static final long serialVersionUID = -7425014359719165469L;
    
    public TrimLeadingZerosType() {
        super(Normalizer.TRIM_LEADING_ZEROS_NORMALIZER);
    }
}
