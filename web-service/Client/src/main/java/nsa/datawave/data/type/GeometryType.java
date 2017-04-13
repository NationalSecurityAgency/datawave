package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;
import nsa.datawave.data.normalizer.OneToManyNormalizer;
import nsa.datawave.data.type.util.Geometry;

import java.util.List;

public class GeometryType extends BaseType<Geometry> implements OneToManyNormalizerType<Geometry> {
    
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
}
