package nsa.datawave;

import java.util.Collection;

public class IdentityDataType implements nsa.datawave.data.type.Type {
    @Override
    public String normalize() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Comparable denormalize() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String normalize(String in) {
        return in;
    }
    
    @Override
    public String normalizeRegex(String in) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Collection<String> expand(String in) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Collection<String> expand() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Comparable denormalize(String in) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setDelegate(Comparable delegate) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setDelegateFromString(String str) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Comparable getDelegate() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setNormalizedValue(String normalizedValue) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String getNormalizedValue() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void normalizeAndSetNormalizedValue(Comparable valueToNormalize) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void validate() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int compareTo(Object o) {
        throw new UnsupportedOperationException();
    }
}
