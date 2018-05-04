package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

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
    
    /**
     * two identical strings + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return PrecomputedSizes.STRING_STATIC_REF * 2 + Sizer.REFERENCE + (4 * normalizedValue.length());
    }
}
