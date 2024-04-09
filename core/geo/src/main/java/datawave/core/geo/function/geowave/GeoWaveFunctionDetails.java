package datawave.core.geo.function.geowave;

import static datawave.core.geo.utils.GeoWaveUtils.GEOWAVE_NAMESPACE;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.utils.GeoWaveUtils;

public class GeoWaveFunctionDetails extends AbstractGeoFunctionDetails {
    private final String name;
    private final Set<String> fields;
    private final List<String> wkt;
    private List<Geometry> geometry;
    private List<Envelope> envelope;

    public GeoWaveFunctionDetails(String name, Set<String> fields, List<String> wkt) {
        this.name = name;
        this.fields = fields;
        this.wkt = wkt;
    }

    @Override
    public String getNamespace() {
        return GEOWAVE_NAMESPACE;
    }

    @Override
    public String getName() {
        return name;
    }

    public Set<String> getFields() {
        return fields;
    }

    public List<String> getWkt() {
        return wkt;
    }

    public List<Geometry> getGeometry() {
        if (geometry == null) {
            geometry = new ArrayList<>();
            for (String wktEntry : wkt) {
                geometry.add(GeoWaveUtils.parseWkt(wktEntry));
            }
        }
        return geometry;
    }

    public List<Envelope> getEnvelope() {
        if (envelope == null) {
            envelope = getGeometry().stream().map(Geometry::getEnvelopeInternal).collect(Collectors.toList());
        }
        return envelope;
    }
}
