package datawave.data.normalizer;

import java.util.Collection;

public class RawDateNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = -3268331784114135470L;
    private DateNormalizer delegate = new DateNormalizer();
    
    @Override
    public String normalize(String fieldValue) {
        return delegate.normalize(fieldValue);
    }
    
    public String normalizeRegex(String fieldRegex) {
        return delegate.normalizeRegex(fieldRegex);
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return delegate.normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        return in;
    }
    
    @Override
    public Collection<String> expand(String dateString) {
        return delegate.expand(dateString);
    }
    
}
