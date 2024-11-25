package datawave.data.normalizer;

import java.util.List;

public interface OneToManyNormalizer<T> extends Normalizer<T> {
    
    List<String> normalizeToMany(String in);
    
    List<String> normalizeDelegateTypeToMany(T foo);
}
