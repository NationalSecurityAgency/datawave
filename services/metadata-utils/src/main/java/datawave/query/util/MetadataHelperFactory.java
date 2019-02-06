package datawave.query.util;

import com.google.common.collect.Maps;
import datawave.query.composite.CompositeMetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class MetadataHelperFactory {
    
    public static final String ALL_AUTHS_PROPERTY = "dw.metadatahelper.all.auths";
    
    public static final Logger log = LoggerFactory.getLogger(MetadataHelperFactory.class);
    
    private final BeanFactory beanFactory;
    private final TypeMetadataHelper.Factory typeMetadataHelperFactory;
    
    public MetadataHelperFactory() {
        this.beanFactory = null;
        this.typeMetadataHelperFactory = null;
    }
    
    public MetadataHelperFactory(BeanFactory beanFactory, TypeMetadataHelper.Factory typeMetadataHelperFactory) {
        this.beanFactory = beanFactory;
        this.typeMetadataHelperFactory = typeMetadataHelperFactory;
    }
    
    public MetadataHelper createMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> fullUserAuths) {
        return createMetadataHelper(connector, metadataTableName, fullUserAuths, false);
    }
    
    /**
     * Creates a {@link MetadataHelper} by retrieving the necessary beans from the injected {@link BeanFactory} and passing along the additional supplied
     * arguments to the constructor(s). The returned bean will be a Spring-managed proxy that wraps certain methods with caching.
     *
     * @param connector
     *            the connector to use when talking to Accumulo
     * @param metadataTableName
     *            the name of the metadata table in Accumulo
     * @param fullUserAuths
     *            the authorizations for the proxied entity chain that is calling this method
     * 
     * @return a new MetadataHelper
     */
    @SuppressWarnings("unchecked")
    public MetadataHelper createMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> fullUserAuths, boolean useTypeSubstitution) {
        if (beanFactory != null) {
            Set<Authorizations> allMetadataAuths = (Set<Authorizations>) beanFactory.getBean("allMetadataAuths",
                            ResolvableType.forClassWithGenerics(Set.class, Authorizations.class).resolve());
            Collection<String> mergedAuths = MetadataHelper.getUsersMetadataAuthorizationSubset(fullUserAuths, allMetadataAuths);
            Set<Authorizations> authSubset = Collections.singleton(new Authorizations(mergedAuths.toArray(new String[0])));
            
            TypeMetadataHelper typeMetadataHelper = typeMetadataHelperFactory.createTypeMetadataHelper(connector, metadataTableName, authSubset,
                            useTypeSubstitution);
            CompositeMetadataHelper compositeMetadataHelper = beanFactory.getBean(CompositeMetadataHelper.class, connector, metadataTableName, authSubset);
            AllFieldMetadataHelper allFieldMetadataHelper = beanFactory.getBean(AllFieldMetadataHelper.class, typeMetadataHelper, compositeMetadataHelper,
                            connector, metadataTableName, authSubset, fullUserAuths);
            return beanFactory.getBean(MetadataHelper.class, allFieldMetadataHelper, allMetadataAuths, connector, metadataTableName, authSubset, fullUserAuths);
        } else {
            log.warn("MetadataHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code");
            if (log.isDebugEnabled())
                log.debug("MetadataHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code",
                                new Exception("exception for debug purposes"));
            
            Map<String,String> typeSubstitutions = new HashMap<>();
            typeSubstitutions.put("datawave.data.type.DateType", "datawave.data.type.RawDateType");
            Set<Authorizations> allMetadataAuths = Collections.singleton(MetadataDefaultsFactory.getDefaultAuthorizations());
            Collection<String> mergedAuths = MetadataHelper.getUsersMetadataAuthorizationSubset(fullUserAuths, allMetadataAuths);
            Set<Authorizations> authSubset = Collections.singleton(new Authorizations(mergedAuths.toArray(new String[0])));
            TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, connector, metadataTableName, authSubset,
                            false);
            CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper(connector, metadataTableName, authSubset);
            AllFieldMetadataHelper allFieldMetadataHelper = new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, connector,
                            metadataTableName, authSubset, fullUserAuths);
            return new MetadataHelper(allFieldMetadataHelper, allMetadataAuths, connector, metadataTableName, authSubset, fullUserAuths);
        }
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
