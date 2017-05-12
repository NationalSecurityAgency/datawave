package datawave.data.type;

import java.io.Serializable;
import java.util.Collection;

import datawave.data.normalizer.Normalizer;

public class BaseType<T extends Comparable<T> & Serializable> implements Serializable, Type<T> {
    
    private static final long serialVersionUID = 5354270429891763693L;
    
    protected T delegate;
    protected String normalizedValue;
    protected final Normalizer<T> normalizer;
    
    public BaseType(String delegateString, Normalizer<T> normalizer) {
        this.normalizer = normalizer;
        setDelegate(normalizer.denormalize(delegateString));
    }
    
    public BaseType(Normalizer<T> normalizer) {
        this.normalizer = normalizer;
    }
    
    public T getDelegate() {
        return delegate;
    }
    
    public void setDelegateFromString(String in) {
        setDelegate(normalizer.denormalize(in));
    }
    
    public void setDelegate(T delegate) {
        this.delegate = delegate;
        normalizeAndSetNormalizedValue(this.delegate);
    }
    
    public String getNormalizedValue() {
        return normalizedValue;
    }
    
    @Override
    public T denormalize() {
        return this.delegate;
    }
    
    public void setNormalizedValue(String normalizedValue) {
        this.normalizedValue = normalizedValue;
    }
    
    public int compareTo(Type<T> o) {
        return this.getDelegate().compareTo(o.getDelegate());
    }
    
    public String normalize() {
        return normalizer.normalizeDelegateType(this.delegate);
    }
    
    public String normalize(String in) {
        return normalizer.normalize(in);
    }
    
    public Collection<String> expand(String in) {
        return normalizer.expand(in);
    }
    
    public Collection<String> expand() {
        return normalizer.expand(this.delegate.toString());
    }
    
    public T denormalize(String in) {
        return normalizer.denormalize(in);
    }
    
    @Override
    public String normalizeRegex(String in) {
        return normalizer.normalizeRegex(in);
    }
    
    @Override
    public void normalizeAndSetNormalizedValue(T valueToNormalize) {
        setNormalizedValue(normalizer.normalizeDelegateType(valueToNormalize));
    }
    
    public void validate() {
        if (this.delegate == null || this.normalizedValue == null)
            throw new IllegalArgumentException(this + " does not validate: " + delegate + "," + normalizedValue);
    }
    
    private int delegateHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((delegate == null) ? 0 : delegate.hashCode());
        return result;
    }
    
    private boolean delegateEquals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        BaseType<T> other = (BaseType<T>) obj;
        if (delegate == null) {
            if (other.delegate != null)
                return false;
        } else if (!delegate.equals(other.delegate))
            return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        if (delegate == null) {
            // Use the concrete Type's full name to ensure that we don't get multiple
            // instances of the same class (as Object#hashCode is based on virtual memory location)
            return this.getClass().getName().hashCode();
        } else {
            return delegateHashCode();
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (delegate == null) {
            Class<?> otherClz = o.getClass();
            
            // Since Types are considered to be stateless,
            // we can treat equality as the same class
            if (otherClz.equals(this.getClass())) {
                return true;
            }
            return false;
        } else {
            return delegateEquals(o);
        }
    }
    
    @Override
    public String getDelegateAsString() {
        return toString();
    }
    
    @Override
    public String toString() {
        return delegate == null ? super.toString() : delegate.toString();
    }
    
}
