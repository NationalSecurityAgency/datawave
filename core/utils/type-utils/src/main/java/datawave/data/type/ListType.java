package datawave.data.type;

import java.util.ArrayList;
import java.util.List;

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
    public List<String> normalizeToMany(String in) {
        String[] splits = StringUtils.split(in, delimiter);
        List<String> strings = new ArrayList(splits.length);
        for (String s : splits) {
            
            String str = normalizer.normalize(s);
            strings.add(str);
            
        }
        
        return strings;
    }
    
    @Override
    public void setDelegateFromString(String in) {
        this.normalizedValues = normalizeToMany(in);
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
