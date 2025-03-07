package datawave.query.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

public enum TypeFilter implements Serializable {
    ALL(null), NONE(Collections.EMPTY_SET), SET(Sets.newHashSet());
    
    private Set<String> dataTypes;
    
    TypeFilter(Set<String> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public void setDataTypes(Set<String> dataTypes) {
        this.dataTypes = dataTypes;
    }
    
    public Set<String> getDataTypes() {
        return dataTypes;
    }
    
}
