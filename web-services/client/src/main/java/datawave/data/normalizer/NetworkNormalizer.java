package datawave.data.normalizer;

public class NetworkNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = 8279399353763569005L;
    
    public String normalize(String fieldValue) {
        String normed = fieldValue;
        
        try {
            normed = IP_ADDRESS_NORMALIZER.normalize(fieldValue);
            
        } catch (Exception iae) {
            /**
             * try as a mac address
             */
            try {
                normed = MAC_ADDRESS_NORMALIZER.normalize(fieldValue);
            } catch (Exception e) {
                /**
                 * ok, default to string normalization
                 */
                normed = LC_NO_DIACRITICS_NORMALIZER.normalize(fieldValue);
            }
        }
        return normed;
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
