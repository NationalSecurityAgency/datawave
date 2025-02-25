package datawave.data.type;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import datawave.data.normalizer.DiscreteIndexNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.OneToManyNormalizer;
import datawave.data.type.util.AbstractGeometry;

/**
 * The base GeoWave geometry type, which provides an implementation for the discrete index type interface.
 *
 * @param <T>
 *            The underlying geometry type
 */
public abstract class AbstractGeometryType<T extends AbstractGeometry & Comparable<T>> extends BaseType<T> implements DiscreteIndexType<T> {
    
    private static final long GEOMETRY_FACTORY_SIZE = 120;
    private static final long ENVELOPE_SIZE = 45;
    private static final long GEOMETRY_BASE_SIZE = ENVELOPE_SIZE + 20;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + Sizer.REFERENCE + GEOMETRY_FACTORY_SIZE;
    
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
    
    @Override
    public long sizeInBytes() {
        long size = STATIC_SIZE + (2 * normalizedValue.length());
        
        if (this instanceof OneToManyNormalizerType) {
            List<String> values = ((OneToManyNormalizerType<?>) this).getNormalizedValues();
            size += 2 * values.stream().map(String::length).map(x -> x + Sizer.REFERENCE).reduce(Integer::sum).orElse(0);
        }
        
        List<Geometry> leafGeometries = new ArrayList<>();
        LinkedList<Geometry> workingList = new LinkedList<>();
        workingList.push(delegate.getJTSGeometry());
        
        while (!workingList.isEmpty()) {
            Geometry geom = workingList.pop();
            
            if (geom.getNumGeometries() > 1) {
                size += Sizer.OBJECT_OVERHEAD;
                
                // push all the geometries to the working list
                for (int i = 0; i < geom.getNumGeometries(); i++) {
                    workingList.push(geom.getGeometryN(i));
                }
            } else if (geom instanceof Polygon) {
                size += 2 * Sizer.OBJECT_OVERHEAD + GEOMETRY_BASE_SIZE;
                
                Polygon poly = (Polygon) geom;
                
                // push all the exterior and interior rings to the working list
                workingList.push(poly.getExteriorRing());
                for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                    workingList.push(poly.getInteriorRingN(i));
                }
                
            } else {
                size += 3 * Sizer.OBJECT_OVERHEAD + GEOMETRY_BASE_SIZE;
                leafGeometries.add(geom);
            }
        }
        
        for (Geometry geom : leafGeometries) {
            size += Sizer.ARRAY_OVERHEAD + Sizer.OBJECT_OVERHEAD + geom.getCoordinates().length * (3 * 8 + Sizer.OBJECT_OVERHEAD + Sizer.REFERENCE)
                            + Sizer.REFERENCE;
        }
        return size;
    }
}
