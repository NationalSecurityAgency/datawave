package datawave.data.type;

import datawave.data.normalizer.DiscreteIndexNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.util.AbstractGeometry;

import java.util.List;

/**
 * The base GeoWave geometry type, which provides an implementation for the discrete index type interface.
 *
 * @param <T>
 *            The underlying geometry type
 */
public abstract class AbstractGeometryType<T extends AbstractGeometry & Comparable<T>> extends BaseType<T> implements DiscreteIndexType<T> {
    
    public AbstractGeometryType(Normalizer<T> normalizer) {
        super(normalizer);
    }
    
    @Override
    public String incrementIndex(String index) {
        return ((DiscreteIndexNormalizer) normalizer).incrementIndex(index);
    }
    
    @Override
    public String decrementIndex(String index) {
        return ((DiscreteIndexNormalizer) normalizer).decrementIndex(index);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public List<String> discretizeRange(String beginIndex, String endIndex) {
        return ((DiscreteIndexNormalizer) normalizer).discretizeRange(beginIndex, endIndex);
    }
    
    @Override
    public boolean producesFixedLengthRanges() {
        return ((DiscreteIndexNormalizer) normalizer).producesFixedLengthRanges();
    }
}
