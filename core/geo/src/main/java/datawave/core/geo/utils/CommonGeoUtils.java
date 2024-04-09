package datawave.core.geo.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;

public class CommonGeoUtils {
    /**
     * Setting the precision too high is unnecessary, and will result in occasional computational errors within the JTS library.
     */
    static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(1000000));

    public static Geometry createGeometryCollection(Collection<Geometry> geometries) {
        return gf.createGeometryCollection(geometries.toArray(new Geometry[0]));
    }

    public static Geometry geometriesToGeometry(List<Geometry> geometries) {
        Geometry geometry = null;
        if (geometries.size() == 1) {
            geometry = geometries.get(0);
        } else if (geometries.size() > 1) {
            geometry = createGeometryCollection(geometries);
        }
        return geometry;
    }

    /**
     * Generates and returns the geo query ranges for the given geometry
     *
     * @param geometry
     *            the query geometry
     * @param type
     *            the type
     * @param config
     *            the geo query config
     * @return a list of terms and ranges
     */
    public static List<String[]> generateTermsAndRanges(Geometry geometry, Type<?> type, GeoQueryConfig config) {
        List<String[]> termsAndRanges = Collections.emptyList();
        if (type instanceof GeometryType || type instanceof PointType) {
            termsAndRanges = GeoWaveUtils.generateTermsAndRanges(geometry, type, config);
        } else if (type instanceof GeoType) {
            termsAndRanges = GeoUtils.generateTermsAndRanges(geometry, config);
        }
        return termsAndRanges;
    }

    /**
     * Decomposes the geometry into its constituent geometries, and clusters together the intersecting constituent geometries. If the number of recombined
     * geometries is less than the max envelopes setting, the recombined geometry envelopes will be returned. Otherwise, the envelope for the original geometry
     * will be returned.
     *
     * @param geometry
     *            the query geometry
     * @param maxEnvelopes
     *            the maximum number of disjoint envelopes
     * @return a list of disjoint envelopes representing the geometry and/or its constituents
     */
    public static List<Envelope> getDisjointEnvelopes(Geometry geometry, int maxEnvelopes) {
        if (geometry.getNumGeometries() > 0) {
            List<Geometry> geometries = getEnvelopeGeometries(geometry);
            List<Geometry> intersectedGeometries = new ArrayList<>();

            if (combineIntersectedGeometries(geometries, intersectedGeometries, maxEnvelopes)) {
                return intersectedGeometries.stream().map(Geometry::getEnvelopeInternal).collect(Collectors.toList());
            }
        }
        return Collections.singletonList(geometry.getEnvelopeInternal());
    }

    /**
     * Decomposes the geometry into its constituent geometries, and gets the envelopes for those geometries
     *
     * @param geometry
     *            the geometry
     * @return a list of envelopes represented as geometries
     */
    private static List<Geometry> getEnvelopeGeometries(Geometry geometry) {
        List<Geometry> geometries = new ArrayList<>();
        if (geometry.getNumGeometries() > 1) {
            for (int geoIdx = 0; geoIdx < geometry.getNumGeometries(); geoIdx++) {
                geometries.addAll(getEnvelopeGeometries(geometry.getGeometryN(geoIdx)));
            }
        } else {
            geometries.add(geometry.getEnvelope());
        }
        return geometries;
    }

    /**
     * Goes through the list of geometries, and combines intersecting geometries until we have reached the desired number of disjoint envelopes.
     *
     * @param geometries
     *            the geometries
     * @param combinedGeometries
     *            the resulting combined geometries
     * @param maxEnvelopes
     *            the maximum number of disjoint envelopes
     * @return true if the number of combined geometries is less than the max envelope limit
     */
    private static boolean combineIntersectedGeometries(List<Geometry> geometries, List<Geometry> combinedGeometries, int maxEnvelopes) {
        while (!geometries.isEmpty() && combinedGeometries.size() < maxEnvelopes) {
            combinedGeometries.add(findIntersectedGeometries(geometries.remove(0), geometries));
        }
        return geometries.isEmpty();
    }

    /**
     * Finds all the geometries that intersect with the source geometry, merges them, and returns the resulting envelope as a geometry.
     *
     * @param sourceGeometry
     *            the source geometry
     * @param geometries
     *            the other geometries, which will be modified by this call
     * @return an envelope represented as a geometry
     */
    private static Geometry findIntersectedGeometries(Geometry sourceGeometry, List<Geometry> geometries) {
        List<Geometry> intersected = new ArrayList<>();

        // find all geometries which intersect with with srcGeom
        Iterator<Geometry> geometryIter = geometries.iterator();
        while (geometryIter.hasNext()) {
            Geometry geometry = geometryIter.next();
            if (geometry.intersects(sourceGeometry)) {
                geometryIter.remove();
                intersected.add(geometry);
            }
        }

        // compute the envelope for the intersected geometries and the source geometry, and look for more intersections
        if (!intersected.isEmpty()) {
            intersected.add(sourceGeometry);
            Geometry mergedGeometry = new GeometryCollection(intersected.toArray(new Geometry[0]), new GeometryFactory()).getEnvelope();
            return findIntersectedGeometries(mergedGeometry, geometries);
        }

        return sourceGeometry;
    }
}
