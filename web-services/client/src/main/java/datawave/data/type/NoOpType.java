package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class NoOpType extends BaseType<String> {
    
    private static final long serialVersionUID = 5316252096230974722L;
    
    public NoOpType() {
        super(Normalizer.NOOP_NORMALIZER);
    }
    
    public NoOpType(String value) {
        this();
        this.setDelegate(value);
        super.setNormalizedValue(normalizer.normalize(value));
    }
}
