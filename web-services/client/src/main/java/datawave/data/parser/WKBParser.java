package datawave.data.parser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import org.jboss.resteasy.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class can be used to parse a geometry object from Base64 encoded well-known binary (WKB).
 */
public class WKBParser extends GeometryParser {
    
    private static final Logger log = LoggerFactory.getLogger(WKBParser.class);
    
    @Override
    public Geometry parseGeometry(String geoString) {
        Geometry geom = null;
        try {
            byte[] wkbBytes = Base64.decode(geoString);
            geom = new WKBReader().read(wkbBytes);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            if (log.isTraceEnabled())
                log.trace("Cannot parse WKB geometry from [" + geoString + "]");
        } catch (IOException e) {
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
