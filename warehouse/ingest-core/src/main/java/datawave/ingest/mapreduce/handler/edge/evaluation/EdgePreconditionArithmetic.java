package datawave.ingest.mapreduce.handler.edge.evaluation;

import datawave.attribute.EventField;
import datawave.attribute.EventFieldValueTuple;
import org.apache.commons.jexl2.JexlArithmetic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class EdgePreconditionArithmetic extends JexlArithmetic {
    
    private Map<String,Set<String>> matchingGroups = new HashMap<>();
    
    public EdgePreconditionArithmetic() {
        super(false);
    }
    
    /*
     * Currently only EQ options are supplied.
     */
    
    @Override
    public boolean equals(final Object left, final Object right) {
        boolean matches = false;
        
        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);
            
            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = EventFieldValueTuple.getValue(tuple);
                if (super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if (!(left instanceof Collection) && (right instanceof Collection)) {
            Object newLeft = EventFieldValueTuple.getValue(left);
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = EventFieldValueTuple.getValue(tuple);
                if (super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if ((left instanceof Collection) && (right instanceof Collection)) {
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object lefttuple = iter.next();
                Iterator iter2 = ((Collection) left).iterator();
                while (iter2.hasNext()) {
                    Object righttuple = iter2.next();
                    Object newLeft = EventFieldValueTuple.getValue(lefttuple);
                    Object newRight = EventFieldValueTuple.getValue(righttuple);
                    if (super.equals(newLeft, newRight)) {
                        addMatchingGroup(righttuple);
                        addMatchingGroup(lefttuple);
                        matches = true;
                    }
                }
            }
        } else {
            Object newLeft = EventFieldValueTuple.getValue(left);
            Object newRight = EventFieldValueTuple.getValue(right);
            
            if (super.equals(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
                
            }
        }
        
        return matches;
    }
    
    @Override
    public boolean matches(Object left, Object right) {
        
        if (left == null && right == null) {
            // if both are null L == R
            return true;
        }
        if (left == null || right == null) {
            // we know both aren't null, therefore L != R
            return false;
        }
        final String arg = left.toString();
        boolean matches = false;
        if (right instanceof java.util.regex.Pattern) {
            matches = ((java.util.regex.Pattern) right).matcher(arg).matches();
            if (matches) {
                addMatchingGroup(left);
            }
        } else {
            matches = arg.matches(right.toString());
        }
        return matches;
    }
    
    @Override
    public boolean lessThan(final Object left, final Object right) {
        boolean matches = false;
        
        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);
            
            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }
        
        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            if (super.lessThan(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
                
            }
        }
        
        return matches;
    }
    
    @Override
    public boolean lessThanOrEqual(final Object left, final Object right) {
        boolean matches = false;
        
        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }
        
        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            if (super.lessThanOrEqual(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
                
            }
        }
        
        return matches;
    }
    
    @Override
    public boolean greaterThan(final Object left, final Object right) {
        boolean matches = false;
        
        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }
        
        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            if (super.greaterThan(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
                
            }
        }
        
        return matches;
    }
    
    @Override
    public boolean greaterThanOrEqual(final Object left, final Object right) {
        boolean matches = false;
        
        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
            
        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            
            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }
        
        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));
            
            if (super.greaterThanOrEqual(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
                
            }
        }
        
        return matches;
    }
    
    private void addMatchingGroup(Object o) {
        if (o instanceof EventFieldValueTuple) {
            String fieldName = EventFieldValueTuple.getFieldName(o);
            String commonality = EventField.getGroup(fieldName);
            String group = EventField.getSubgroup(fieldName);
            Set<String> groups = matchingGroups.get(commonality);
            if (groups == null) {
                groups = new HashSet<>();
                groups.add(group);
                matchingGroups.put(commonality, groups);
            } else
                groups.add(group);
        }
    }
    
    public Map<String,Set<String>> getMatchingGroups() {
        return matchingGroups;
    }
    
    public void clearMatchingGroups() {
        matchingGroups = new HashMap<>();
    }
    
}
