package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class LcType extends BaseType<String> {
    
    private static final long serialVersionUID = -5102714749195917406L;
    
    public LcType() {
        super(Normalizer.LC_NORMALIZER);
    }
    
    public LcType(String delegateString) {
        super(delegateString, Normalizer.LC_NORMALIZER);
    }
    
}
