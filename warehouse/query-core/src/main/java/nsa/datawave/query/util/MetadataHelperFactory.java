package nsa.datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class MetadataHelperFactory {
    
    public static final Logger log = LoggerFactory.getLogger(MetadataHelperFactory.class);
    
    /**
     * this is method-injected in the QueryLogicFactory.xml file so that it will be overridden to return a MetadataHelper from the spring context (and that one
     * will have caching)
     * 
     * @return
     */
    public MetadataHelper createMetadataHelper() {
        return MetadataHelper.getInstance();
    }
}
