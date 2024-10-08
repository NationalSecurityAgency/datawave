package datawave;

import java.util.Collection;

import datawave.data.type.Type;

public class IdentityDataType implements Type<String> {
    @Override
    public String normalize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String denormalize() {
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

    // @Override
    public boolean normalizedRegexIsLossy(String in) {
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
    public String denormalize(String in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDelegate(String delegate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDelegateAsString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDelegateFromString(String str) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDelegate() {
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
    public void normalizeAndSetNormalizedValue(String valueToNormalize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Type<String> o) {
        throw new UnsupportedOperationException();
    }
}
