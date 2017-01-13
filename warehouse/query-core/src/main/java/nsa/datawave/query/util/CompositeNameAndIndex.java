package nsa.datawave.query.util;

/**
 * convenience class to act as a value for a composite key in a map COLOR maps to Composite(COLOR_WHEELS,0) and Composite(MAKE_COLOR,1) in a multimap
 */
public class CompositeNameAndIndex implements Comparable<CompositeNameAndIndex> {
    
    public final String compositeName;
    public final Integer fieldIndex;
    
    public CompositeNameAndIndex(String compositeName, Integer fieldIndex) {
        this.compositeName = compositeName;
        this.fieldIndex = fieldIndex;
    }
    
    public CompositeNameAndIndex(String csv) {
        int idx = csv.indexOf(',');
        if (idx < 0)
            throw new IllegalArgumentException("Attempt to create Composite without csv:" + csv);
        this.compositeName = csv.substring(0, idx);
        this.fieldIndex = Integer.parseInt(csv.substring(idx + 1));
    }
    
    @Override
    public int compareTo(CompositeNameAndIndex that) {
        int compare = this.fieldIndex.compareTo(that.fieldIndex);
        if (compare == 0) {
            return this.compositeName.compareTo(that.compositeName);
        } else {
            return compare;
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fieldIndex == null) ? 0 : fieldIndex.hashCode());
        result = prime * result + ((compositeName == null) ? 0 : compositeName.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CompositeNameAndIndex other = (CompositeNameAndIndex) obj;
        if (fieldIndex == null) {
            if (other.fieldIndex != null)
                return false;
        } else if (!fieldIndex.equals(other.fieldIndex))
            return false;
        if (compositeName == null) {
            if (other.compositeName != null)
                return false;
        } else if (!compositeName.equals(other.compositeName))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return compositeName + "," + fieldIndex;
    }
}
