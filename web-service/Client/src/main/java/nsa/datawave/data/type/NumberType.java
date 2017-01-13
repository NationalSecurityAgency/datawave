package nsa.datawave.data.type;

import java.math.BigDecimal;

import nsa.datawave.data.normalizer.Normalizer;

public class NumberType extends BaseType<BigDecimal> {
    
    private static final long serialVersionUID = 1398451215614987988L;
    
    public NumberType() {
        super(Normalizer.NUMBER_NORMALIZER);
    }
    
    public NumberType(String delegateString) {
        super(delegateString, Normalizer.NUMBER_NORMALIZER);
    }
}
