package datawave.core.geo.function.geo;

import static datawave.core.geo.function.geo.AbstractQueryGeometry.MAX_LON;
import static datawave.core.geo.function.geo.AbstractQueryGeometry.MIN_LON;
import static datawave.core.geo.utils.GeoUtils.GEO_NAMESPACE;
import static datawave.core.geo.utils.GeoUtils.latLonToIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.function.geo.AbstractQueryGeometry.BoundingBox;
import datawave.core.geo.function.geo.AbstractQueryGeometry.BoundingCircle;

public class ZIndexGeoFunctionDetails extends AbstractGeoFunctionDetails {
    private final String name;
    private final Set<String> fields;
    private final List<AbstractQueryGeometry> queryGeometry;
    private List<Geometry> geometry;
    private List<Envelope> envelope;

    public ZIndexGeoFunctionDetails(String name, Set<String> fields, double minLon, double maxLon, double minLat, double maxLat) {
        this.name = name;
        this.fields = fields;

        // is the lower left longitude greater than the upper right longitude?
        // if so, we have crossed the anti-meridian and should split
        queryGeometry = new ArrayList<>();
        if (minLon > maxLon) {
            queryGeometry.add(new BoundingBox(minLon, MAX_LON, minLat, maxLat));
            queryGeometry.add(new BoundingBox(MIN_LON, maxLon, minLat, maxLat));
        } else {
            queryGeometry.add(new BoundingBox(minLon, maxLon, minLat, maxLat));
        }
    }

    public ZIndexGeoFunctionDetails(String name, Set<String> fields, double centerLon, double centerLat, double radius) {
        this.name = name;
        this.fields = fields;

        queryGeometry = new ArrayList<>();
        queryGeometry.add(new BoundingCircle(centerLon, centerLat, radius));
    }

    @Override
    public String getNamespace() {
        return GEO_NAMESPACE;
    }

    @Override
    public String getName() {
        return name;
    }

    public Set<String> getFields() {
        return fields;
    }

    public List<AbstractQueryGeometry> getQueryGeometry() {
        return queryGeometry;
    }

    @Override
    public List<Geometry> getGeometry() {
        if (geometry == null) {
            geometry = queryGeometry.stream().map(AbstractQueryGeometry::getGeometry).collect(Collectors.toList());
        }
        return geometry;
    }

    @Override
    public List<Envelope> getEnvelope() {
        if (envelope == null) {
            envelope = queryGeometry.stream().map(AbstractQueryGeometry::getEnvelope).collect(Collectors.toList());
        }
        return envelope;
    }
}
