package datawave.data.type;

import java.math.BigDecimal;

import datawave.data.normalizer.Normalizer;

public class NumberType extends BaseType<BigDecimal> {
    
    private static final long serialVersionUID = 1398451215614987988L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + PrecomputedSizes.BIGDECIMAL_STATIC_REF + Sizer.REFERENCE;
    
    public NumberType() {
        super(Normalizer.NUMBER_NORMALIZER);
    }
    
    public NumberType(String delegateString) {
        super(delegateString, Normalizer.NUMBER_NORMALIZER);
    }
    
    /**
     * one String, one BigDecimal and one reference to a normalizer
     */
    @Override
    public long sizeInBytes() {
        return STATIC_SIZE + (2 * normalizedValue.length());
    }
}
