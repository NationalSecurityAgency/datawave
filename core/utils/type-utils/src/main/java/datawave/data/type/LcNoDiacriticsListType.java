package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class LcNoDiacriticsListType extends ListType {
    
    public LcNoDiacriticsListType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
    public LcNoDiacriticsListType(String delegateString) {
        super(delegateString, Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
}
