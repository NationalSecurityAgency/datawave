package datawave.query.testframework;

import datawave.query.testframework.GroupsDataType.GroupField;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GroupsIndexConfiguration extends AbstractFields {
    
    private static final Collection<String> index = new HashSet<>();
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = new HashSet<>();
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    static {
        // set index configuration values
        index.add(GroupField.CITY_EAST.getQueryField());
        index.add(GroupField.STATE_EAST.getQueryField());
        index.add(GroupField.COUNT_EAST.getQueryField());
        
        // composite indexes
        Set<String> cityState = new HashSet<>();
        cityState.add(GroupField.CITY_EAST.getQueryField());
        cityState.add(GroupField.STATE_EAST.getQueryField());
        composite.add(cityState);
        Set<String> count = new HashSet<>(cityState);
        count.add(GroupField.COUNT_EAST.getQueryField());
        composite.add(count);
        Set<String> cityCount = new HashSet<>();
        cityCount.add(GroupField.COUNT_EAST.getQueryField());
        cityCount.add(GroupField.STATE_EAST.getQueryField());
        composite.add(cityCount);
        
        // reverse index
        reverse.add(GroupField.COUNT_EAST.getQueryField());
    }
    
    public GroupsIndexConfiguration() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return "GroupsIndexConfiguration{" + super.toString() + "}";
    }
}
