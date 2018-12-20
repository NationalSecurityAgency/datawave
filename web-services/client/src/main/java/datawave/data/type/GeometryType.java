package datawave.data.type;

import datawave.data.normalizer.DiscreteIndexNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.OneToManyNormalizer;
import datawave.data.type.util.Geometry;

import java.util.List;

public class GeometryType extends BaseType<Geometry> implements OneToManyNormalizerType<Geometry>, DiscreteIndexType<Geometry> {
    
    protected List<String> normalizedValues;
    
    public GeometryType() {
        super(Normalizer.GEOMETRY_NORMALIZER);
    }
    
    public List<String> normalizeToMany(String in) {
        return ((OneToManyNormalizer<Geometry>) normalizer).normalizeToMany(in);
    }
    
    public void setNormalizedValues(List<String> normalizedValues) {
        this.normalizedValues = normalizedValues;
        setNormalizedValue(this.normalizedValues.toString());
    }
    
    @Override
    public void normalizeAndSetNormalizedValue(Geometry valueToNormalize) {
        setNormalizedValues(((OneToManyNormalizer<Geometry>) normalizer).normalizeDelegateTypeToMany(valueToNormalize));
    }
    
    public List<String> getNormalizedValues() {
        return normalizedValues;
    }
    
    @Override
    public String incrementIndex(String index) {
        return ((DiscreteIndexNormalizer) Normalizer.GEOMETRY_NORMALIZER).incrementIndex(index);
    }
    
    @Override
    public String decrementIndex(String index) {
        return ((DiscreteIndexNormalizer) Normalizer.GEOMETRY_NORMALIZER).decrementIndex(index);
    }
    
    @Override
    public List<String> discretizeRange(String beginIndex, String endIndex) {
        return ((DiscreteIndexNormalizer) Normalizer.GEOMETRY_NORMALIZER).discretizeRange(beginIndex, endIndex);
    }
    
    @Override
    public boolean producesFixedLengthRanges() {
        return ((DiscreteIndexNormalizer) Normalizer.GEOMETRY_NORMALIZER).producesFixedLengthRanges();
    }
}
