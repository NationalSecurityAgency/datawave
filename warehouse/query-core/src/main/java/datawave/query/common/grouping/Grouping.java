package datawave.query.common.grouping;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.util.HashMap;
import java.util.Map;

public class Grouping {
    
    private final Map<String,Type<?>> groupingKey = new HashMap<>();
    private final Multimap<String,ColumnVisibility> fieldVisibilities = HashMultimap.create();
    private final Multimap<String,String> groupContextToInstances = HashMultimap.create();
    private int count;
    
    public void Grouping() {}
    
}
