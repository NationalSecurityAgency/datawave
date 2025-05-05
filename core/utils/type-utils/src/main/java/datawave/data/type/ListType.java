package datawave.data.type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.Pair;

import datawave.data.normalizer.Normalizer;
import datawave.util.StringUtils;

public abstract class ListType extends BaseType implements OneToManyNormalizerType {
    protected static final String delimiter = ",|;";
    List<String> normalizedValues;
    
    public ListType(Normalizer normalizer) {
        super(normalizer);
    }
    
    public ListType(String delegateString, Normalizer normalizer) {
        super(delegateString, normalizer);
    }
    
    @Override
    public List<Pair<String,Category>> normalizeToMany(String in) {
        String[] splits = StringUtils.split(in, delimiter);
        List<Pair<String,Category>> strings = new ArrayList(splits.length);
        for (String s : splits) {
            
            String str = normalizer.normalize(s);
            strings.add(new Pair(str, Category.LIST_ELEMENT));
            
        }
        
        return strings;
    }
    
    @Override
    public void setDelegateFromString(String in) {
        this.normalizedValues = normalizeToMany(in).stream().map(Pair::getFirst).collect(Collectors.toList());
        this.delegate = in;
        setNormalizedValue(in);
    }
    
    @Override
    public List<String> getNormalizedValues() {
        return normalizedValues;
    }
    
    @Override
    public boolean expandAtQueryTime() {
        return false;
    }
}
