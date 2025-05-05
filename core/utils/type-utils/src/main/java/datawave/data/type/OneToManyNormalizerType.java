package datawave.data.type;

import java.util.List;

import org.apache.accumulo.core.util.Pair;

public interface OneToManyNormalizerType<T extends Comparable<T>> extends Type<T> {
    
    List<Pair<String,Category>> normalizeToMany(String in);
    
    List<String> getNormalizedValues();
    
    boolean expandAtQueryTime();
}
