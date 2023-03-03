package datawave.query.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateIndexHelperFactory {
    
    public static final Logger log = LoggerFactory.getLogger(DateIndexHelperFactory.class);
    
    /**
     * this is method-injected in the QueryLogicFactory.xml file so that it will be overridden to return a MetadataHelper from the spring context (and that one
     * will have caching)
     * 
     * @return a date index helper
     */
    public DateIndexHelper createDateIndexHelper() {
        return DateIndexHelper.getInstance();
    }
}
