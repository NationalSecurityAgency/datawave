package datawave.data.normalizer;

public class TrimLeadingZerosNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = -5681890794025882300L;
    
    public String normalize(String fv) {
        int len = fv.length();
        int index;
        for (index = 0; (index < len) && (fv.charAt(index) == '0'); index++)
            ;
        if (index > 0) {
            fv = fv.substring(index);
        }
        return fv;
    }
    
    /**
     * Note that we really cannot normalize the regex here, so the regex must work against the normalized and unnormalized forms.
     */
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
