package datawave.data.normalizer;

import java.math.BigDecimal;
import java.util.regex.Pattern;

import datawave.data.type.util.NumericalEncoder;

public class NumberNormalizer extends AbstractNormalizer<BigDecimal> {
    
    private static final long serialVersionUID = -2781476072987375820L;
    
    public String normalize(String fv) {
        try {
            return NumericalEncoder.encode(fv);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to normalize value as a number: " + fv);
        }
    }
    
    /**
     * We cannot support regex against numbers
     */
    public String normalizeRegex(String fieldRegex) {
        try {
            String fixedNumber = normalize(fieldRegex);
            return Pattern.quote(fixedNumber);
        } catch (IllegalArgumentException e1) {
            throw new IllegalArgumentException("Cannot normalize a regex against a numeric field pattern");
        }
    }
    
    @Override
    public String normalizeDelegateType(BigDecimal delegateIn) {
        return normalize(delegateIn.toString());
    }
    
    @Override
    public BigDecimal denormalize(String in) {
        if (NumericalEncoder.isPossiblyEncoded(in)) {
            try {
                return NumericalEncoder.decode(in);
            } catch (NumberFormatException e) {
                // not encoded...
            }
        }
        return new BigDecimal(in);
    }
    
}
