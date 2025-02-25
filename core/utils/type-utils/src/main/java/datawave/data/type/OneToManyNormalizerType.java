package datawave.data.type;

import java.util.List;

public interface OneToManyNormalizerType<T extends Comparable<T>> extends Type<T> {
    
    List<String> normalizeToMany(String in);
    
    List<String> getNormalizedValues();
    
    boolean expandAtQueryTime();
}
