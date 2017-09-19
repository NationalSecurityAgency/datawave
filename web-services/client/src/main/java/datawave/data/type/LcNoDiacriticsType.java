package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class LcNoDiacriticsType extends BaseType<String> {
    
    private static final long serialVersionUID = -6219894926244790742L;
    
    public LcNoDiacriticsType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
    public LcNoDiacriticsType(String delegateString) {
        super(delegateString, Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
}
