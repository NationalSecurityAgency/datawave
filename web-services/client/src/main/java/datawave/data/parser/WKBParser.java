package datawave.data.parser;

import com.google.common.io.BaseEncoding;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used to parse a geometry object from Base64 encoded well-known binary (WKB).
 */
public class WKBParser extends GeometryParser {
    
    private static final Logger log = LoggerFactory.getLogger(WKBParser.class);
    
    @Override
    public Geometry parseGeometry(String geoString) {
        Geometry geom = null;
        try {
            byte[] wkbBytes = BaseEncoding.base64().decode(geoString);
            geom = new WKBReader().read(wkbBytes);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            if (log.isTraceEnabled())
                log.trace("Cannot parse WKB geometry from [" + geoString + "]");
        }
        return geom;
    }
    
    @Override
    protected int getPriority() {
        return DEFAULT_PRIORITY + 1;
    }
}
