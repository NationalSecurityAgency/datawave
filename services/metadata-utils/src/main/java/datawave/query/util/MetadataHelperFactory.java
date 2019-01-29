package datawave.query.util;

import com.google.common.collect.Maps;
import datawave.query.composite.CompositeMetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class MetadataHelperFactory {
    
    public static final String ALL_AUTHS_PROPERTY = "dw.metadatahelper.all.auths";
    
    public static final Logger log = LoggerFactory.getLogger(MetadataHelperFactory.class);
    
    /**
     * this is method-injected in the QueryLogicFactory.xml file so that it will be overridden to return a MetadataHelper from the spring context (and that one
     * will have caching)
     * 
     * @return
     */
    @Lookup
    public MetadataHelper createMetadataHelper() {
        log.warn("MetadataHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code");
        if (log.isDebugEnabled())
            log.debug("MetadataHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code",
                            new Exception("exception for debug purposes"));
        
        Map<String,String> typeSubstitutions = new HashMap<>();
        typeSubstitutions.put("datawave.data.type.DateType", "datawave.data.type.RawDateType");
        Set<Authorizations> allMetadataAuths = Collections.singleton(MetadataDefaultsFactory.getDefaultAuthorizations());
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths);
        CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper();
        AllFieldMetadataHelper allFieldMetadataHelper = new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper);
        return new MetadataHelper(allFieldMetadataHelper, allMetadataAuths);
    }
    
    @Lookup
    public MetadataHelper createMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> auths) {
        MetadataHelper metadataHelper = createMetadataHelper();
        metadataHelper.initialize(connector, metadataTableName, auths);
        return metadataHelper;
    }
    
    /**
     * Factory primarily for injecting default authorizations that may be needed when there is no Spring injection to fall back on. Previously default auths
     * were hard-coded above, limiting portability of the code.
     */
    private static class MetadataDefaultsFactory {
        static Authorizations getDefaultAuthorizations() {
            String defaultAuths = System.getProperty(ALL_AUTHS_PROPERTY);
            if (null == defaultAuths || defaultAuths.isEmpty()) {
                log.info("No default authorizations are defined. Hopefully the empty set will suffice");
                return new Authorizations();
            } else {
                return new Authorizations(defaultAuths.split(","));
            }
        }
    }
    
}
