package datawave.data.normalizer;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractNormalizer<T> implements Normalizer<T> {
    
    @Override
    public Collection<String> expand(String in) {
        return Collections.singletonList(normalize(in));
    }
    
    @Override
    public boolean normalizedRegexIsLossy(String in) {
        return false;
    }
}
