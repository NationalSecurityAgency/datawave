package datawave.query.jexl;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.jexl3.MapContext;

import datawave.query.collections.FunctionalSet;

public class DatawaveJexlContext extends MapContext {

    private final Comparator<?> valueComparator;
    private final Map<String,Object> map;

    public DatawaveJexlContext() {
        this(new HashMap<>(), null);
    }

    public DatawaveJexlContext(Comparator<?> comparator) {
        this(new HashMap<>(), comparator);
    }

    private DatawaveJexlContext(Map<String,Object> map, Comparator<?> valueComparator) {
        super(map);
        this.map = map;
        this.valueComparator = valueComparator;
    }

    /**
     * Clears the map
     */
    public void clear() {
        this.map.clear();
    }

    public int size() {
        return this.map.size();
    }

    @Override
    public void set(String name, Object value) {
        if (valueComparator != null) {
            if (value instanceof FunctionalSet) {
                value = new FunctionalSet((FunctionalSet) value, valueComparator);
            } else if (value instanceof Collection) {
                TreeSet set = new TreeSet(valueComparator);
                set.addAll((Collection) value);
                value = set;
            }
        }
        super.set(name, value);
    }

    @Override
    // why is this not implemented in MapContext
    public boolean equals(Object o) {
        if (!(o instanceof DatawaveJexlContext)) {
            return false;
        }

        DatawaveJexlContext other = (DatawaveJexlContext) o;

        if (this.size() != other.size()) {
            return false;
        }
        // why not compare the maps?
        Iterator<String> keys = this.map.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            if (!other.has(key)) {
                return false;
            }

            if (!this.get(key).equals(other.get(key))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    public Object get(String name) {
        // So the question is whether a mapping to nothing should return 'null' or an empty collection...
        // If we return an empty collection, then our tests that expect 'null' will need to change
        // If we return a 'null', then our tests that invoke a method on the result of the context lookup will cause a NPE
        // For now, leave it returning 'null', with the below fix in DatawaveInterpreter's visit(ASTMethodNode):
        //
        // public Object visit(ASTMethodNode node, Object data) {
        // if(data == null) {
        // data = new FunctionalSet(); // an empty set
        // }
        // return super.visit(node, data);
        // }
        // -- end---

        // if(got == null) {
        // return FunctionalSet.empty();
        // }
        return map.get(name);
    }
}
