package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class NoOpType extends BaseType<String> {
    
    private static final long serialVersionUID = 5316252096230974722L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE;
    
    public NoOpType() {
        super(Normalizer.NOOP_NORMALIZER);
    }
    
    public NoOpType(String value) {
        this();
        this.setDelegate(value);
        super.setNormalizedValue(normalizer.normalize(value));
    }
    
    /**
     * two identical strings + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (4 * normalizedValue.length());
    }
}
