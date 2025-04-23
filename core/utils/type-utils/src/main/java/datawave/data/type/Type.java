package datawave.data.type;

import java.util.Collection;

public interface Type<T extends Comparable<T>> extends Comparable<Type<T>> {
    
    String normalize();
    
    T denormalize();
    
    String normalize(String in);
    
    String normalizeRegex(String in);
    
    boolean normalizedRegexIsLossy(String in);
    
    Collection<String> expand(String in);
    
    Collection<String> expand();
    
    T denormalize(String in);
    
    void setDelegate(T delegate);
    
    /**
     * The string form must preserve all information in the delegate such that setDelegateFromString will recreate this instance correctly.
     */
    String getDelegateAsString();
    
    void setDelegateFromString(String str);
    
    T getDelegate();
    
    void setNormalizedValue(String normalizedValue);
    
    String getNormalizedValue();
    
    void normalizeAndSetNormalizedValue(T valueToNormalize);
    
    void validate();
    
    class Factory {
        
        private Factory() {
            // private constructor to enforce static access
        }
        
        public static Type<?> createType(String datawaveTypeClassName) {
            try {
                return (Type<?>) Class.forName(datawaveTypeClassName).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Error creating instance of class " + datawaveTypeClassName + ':' + e.getLocalizedMessage(), e);
            }
        }
    }
}
