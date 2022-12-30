package datawave.query.postprocessing.tf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ContentFunction {
    
    private final String field;
    private final Set<String> values;
    
    public ContentFunction(String field, Collection<String> values) {
        this.field = field;
        this.values = new HashSet<>(values);
    }
    
    public String getField() {
        return this.field;
    }
    
    public boolean containsValue(String value) {
        return this.values.contains(value);
    }
    
    public Set<String> getValues() {
        return this.values;
    }
}
