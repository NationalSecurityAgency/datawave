package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class StringType extends BaseType<String> {
    
    private static final long serialVersionUID = 8143572646109171126L;
    
    public StringType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }
    
    /**
     * Two String + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return PrecomputedSizes.STRING_STATIC_REF * 2 + (2 * normalizedValue.length()) + (2 * delegate.length()) + Sizer.REFERENCE;
    }
}
