package datawave.data.type;

import java.util.Collection;

public interface Type<T extends Comparable<T>> extends Comparable<Type<T>> {
    
    String normalize();
    
    T denormalize();
    
    String normalize(String in);
    
    String normalizeRegex(String in);
    
    Collection<String> expand(String in);
    
    Collection<String> expand();
    
    T denormalize(String in);
    
    void setDelegate(T delegate);
    
    void setDelegateFromString(String str);
    
    T getDelegate();
    
    void setNormalizedValue(String normalizedValue);
    
    String getNormalizedValue();
    
    void normalizeAndSetNormalizedValue(T valueToNormalize);
    
    void validate();
    
    class Factory {
        
        public static Type<?> createType(String datawaveTypeClassName) {
            
            try {
                return (Type<?>) Class.forName(datawaveTypeClassName).newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Error creating instance of class " + datawaveTypeClassName + ':' + e.getLocalizedMessage(), e);
            }
        }
    }
}
