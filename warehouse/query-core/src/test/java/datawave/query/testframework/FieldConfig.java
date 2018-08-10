package datawave.query.testframework;

import java.util.Collection;
import java.util.Set;

/**
 * Defines the manner in which the test data fields can be customized. Implementations of this interface will allow for various settings for the index, reverse
 * index, composite index, virtual index, and multi value fields to be configured for testing. Methods for adding and removing will allow for base
 * configurations to be created and then reused.
 * <p>
 * NOTE: When designating fields as multivalue, the field must also designated as a multivalue field for the datatype to extract the correct expected results.
 * </p>
 * <p>
 * Known Errors: Composite fields that are designated as multivalue and contain multiple values will only work correctly for the first value of the sorted
 * entries. Fields that are designated as multivalue but only contain a single entry will work correctly.
 * </p>>
 */
public interface FieldConfig {
    
    Set<String> getIndexFields();
    
    void addIndexField(String field);
    
    void removeIndexField(String field);
    
    Set<String> getIndexOnlyFields();
    
    void addIndexOnlyField(String field);
    
    void removeIndexOnlyField(String field);
    
    Set<String> getReverseIndexFields();
    
    void addReverseIndexField(String field);
    
    void removeReverseIndexField(String field);
    
    Collection<Set<String>> getCompositeFields();
    
    void addCompositeField(Set<String> fields);
    
    void removeCompositeField(Set<String> field);
    
    Collection<Set<String>> getVirtualFields();
    
    void addVirtualField(Set<String> fields);
    
    void removeVirtualField(Set<String> field);
    
    Set<String> getMultiValueFields();
    
    void addMultiValueField(String field);
    
    void removeMultiValueField(String field);
}
