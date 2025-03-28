package datawave.data.parser;

import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WKTParser extends GeometryParser {
    
    private static final Logger log = LoggerFactory.getLogger(WKTParser.class);
    
    private static final String[] geomTypes = new String[] {"GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
            "GEOMETRYCOLLECTION", "CIRCULARSTRING", "COMPOUNDCURVE", "CURVEPOLYGON", "MULTICURVE", "MULTISURFACE", "CURVE", "SURFACE", "POLYHEDRALSURFACE",
            "TIN", "TRIANGLE"};
    private static final String[] zGeomTypes = new String[geomTypes.length];
    
    static {
        for (int i = 0; i < geomTypes.length; i++)
            zGeomTypes[i] = geomTypes[i] + " Z";
    }
    
    @Override
    public Geometry parseGeometry(String geoString) {
        Geometry geom = null;
        try {
            geom = new WKTReader().read(StringUtils.replaceEach(geoString, zGeomTypes, geomTypes));
        } catch (Exception e) {
            if (log.isTraceEnabled())
                log.trace("Cannot parse WKT geometry from [" + geoString + "]");
        }
        return geom;
    }
    
    @Override
    protected int getPriority() {
        return DEFAULT_PRIORITY;
    }
}
