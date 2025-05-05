package datawave.data.normalizer;

import java.util.List;

import org.apache.accumulo.core.util.Pair;

import datawave.data.type.Type;

public interface OneToManyNormalizer<T> extends Normalizer<T> {
    
    List<Pair<String,Type.Category>> normalizeToMany(String in);
    
    List<Pair<String,Type.Category>> normalizeDelegateTypeToMany(T foo);
}
