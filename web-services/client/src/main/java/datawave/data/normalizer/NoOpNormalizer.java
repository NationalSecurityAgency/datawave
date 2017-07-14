package datawave.data.normalizer;

/**
 * 
 */
public class NoOpNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = -2599171413081079348L;
    
    public String normalize(String fieldValue) {
        return fieldValue;
    }
    
    public String normalizeRegex(String fieldRegex) {
        return fieldRegex;
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        return in;
    }
}
