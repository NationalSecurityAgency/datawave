package datawave.query.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.data.ColumnFamilyConstants;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;

@EnableCaching
@Component("typeMetadataHelper")
@Scope("prototype")
public class TypeMetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(TypeMetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";
    
    protected final List<Text> metadataTypeColfs = Collections.singletonList(ColumnFamilyConstants.COLF_T);
    
    protected final AccumuloClient accumuloClient;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    protected final boolean useTypeSubstitution;
    protected final Map<String,String> typeSubstitutions;
    protected final Set<Authorizations> allMetadataAuths;
    
    /**
     * Initializes the instance with a provided update interval.
     *
     * @param client
     *            A client connection to Accumulo
     * @param metadataTableName
     *            The name of the DatawaveMetadata table
     * @param auths
     *            Any {@link Authorizations} to use
     */
    public TypeMetadataHelper(@Qualifier("typeSubstitutions") Map<String,String> typeSubstitutions,
                    @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths, AccumuloClient client, String metadataTableName,
                    Set<Authorizations> auths, boolean useTypeSubstitution) {
        this.typeSubstitutions = (typeSubstitutions == null) ? Maps.newHashMap() : typeSubstitutions;
        this.allMetadataAuths = (allMetadataAuths == null) ? Collections.emptySet() : allMetadataAuths;
        
        Preconditions.checkNotNull(client, "A valid AccumuloClient is required by TypeMetadataHelper");
        this.accumuloClient = client;
        
        Preconditions.checkNotNull(metadataTableName, "The metadata table name is required by TypeMetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Accumulo scan Authorizations are required by TypeMetadataHelper");
        this.auths = auths;
        
        this.useTypeSubstitution = useTypeSubstitution;
        
        log.trace("Constructor connector: {} with auths: {} and metadata table name: {}", accumuloClient.getClass().getCanonicalName(), auths,
                        metadataTableName);
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    @Cacheable(value = "getTypeMetadata", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public TypeMetadata getTypeMetadata() throws TableNotFoundException {
        if (log.isDebugEnabled())
            log.debug("cache fault for getTypeMetadata(" + this.auths + "," + this.metadataTableName + ")");
        return this.getTypeMetadata(null);
    }
    
    @Cacheable(value = "getTypeMetadata", key = "{#root.target.auths,#root.target.metadataTableName,#datatypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public TypeMetadata getTypeMetadata(Set<String> datatypeFilter) throws TableNotFoundException {
        if (log.isDebugEnabled())
            log.debug("cache fault for getTypeMetadata(" + this.auths + "," + this.metadataTableName + "," + datatypeFilter + ")");
        return this.getTypeMetadata(this.auths, this.metadataTableName, datatypeFilter);
        
    }
    
    public Map<Set<String>,TypeMetadata> getTypeMetadataMap(Collection<Authorizations> allAuths) throws TableNotFoundException {
        Collection<Set<String>> powerset = getAllMetadataAuthsPowerSet(allAuths);
        if (log.isTraceEnabled()) {
            log.trace("powerset:" + powerset);
            for (Set<String> s : powerset) {
                log.trace("powerset has :" + s);
            }
        }
        Map<Set<String>,TypeMetadata> map = Maps.newHashMap();
        
        for (Set<String> a : powerset) {
            if (log.isTraceEnabled())
                log.trace("get TypeMetadata with auths:" + a);
            
            Authorizations at = new Authorizations(a.toArray(new String[a.size()]));
            
            if (log.isTraceEnabled())
                log.trace("made an Authorizations:" + at);
            TypeMetadata tm = this.getTypeMetadataForAuths(Collections.singleton(at));
            map.put(a, tm);
        }
        return map;
    }
    
    private Set<Set<String>> getAllMetadataAuthsPowerSet(Collection<Authorizations> allMetadataAuthsCollection) {
        
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(allMetadataAuthsCollection);
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        Set<String> minimizedUserAuths = StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet());
        if (log.isTraceEnabled())
            log.trace("minimizedUserAuths:" + minimizedUserAuths + " with size " + minimizedUserAuths.size());
        Set<Set<String>> powerset = Sets.powerSet(minimizedUserAuths);
        Set<Set<String>> set = Sets.newHashSet();
        for (Set<String> sub : powerset) {
            Set<String> serializableSet = Sets.newHashSet(sub);
            set.add(serializableSet);
        }
        return set;
    }
    
    public TypeMetadata getTypeMetadataForAuths(Set<Authorizations> authSet) throws TableNotFoundException {
        if (log.isTraceEnabled())
            log.trace("getTypeMetadataForAuths(" + authSet + ")");
        return this.getTypeMetadata(authSet, this.metadataTableName, null);
    }
    
    private TypeMetadata getTypeMetadata(Set<Authorizations> auths, String metadataTableName, Set<String> datatypeFilter) throws TableNotFoundException {
        TypeMetadata typeMetadata = new TypeMetadata();
        
        // Scanner to the provided metadata table
        if (log.isTraceEnabled()) {
            log.trace("client:" + accumuloClient + ", metadataTableName:" + metadataTableName + ", auths:" + auths);
            Collection<Authorizations> got = AuthorizationsMinimizer.minimize(auths);
            log.trace("got:" + got + " and it is a " + (got == null ? null : got.getClass()));
        }
        
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        Range range = new Range();
        bs.setRange(range);
        
        // Fetch all the column
        for (Text colf : metadataTypeColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        for (Entry<Key,Value> entry : bs) {
            // Get the column qualifier from the key. It contains the datatype
            // and normalizer class
            if (null != entry.getKey().getColumnQualifier()) {
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                if (idx != -1) {
                    String field = entry.getKey().getRow().toString();
                    String type = colq.substring(0, idx);
                    String className = null;
                    if (datatypeFilter == null || datatypeFilter.isEmpty()) { // no filtering, getem all
                        className = colq.substring(idx + 1);
                    } else if (datatypeFilter.contains(type)) {
                        className = colq.substring(idx + 1);
                    }
                    if (className != null) {
                        if (this.useTypeSubstitution && this.typeSubstitutions.containsKey(className)) {
                            className = this.typeSubstitutions.get(className);
                        }
                        typeMetadata.put(field, type, className);
                    }
                    
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                }
            } else {
                log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
            }
        }
        
        bs.close();
        
        return typeMetadata;
    }
    
    @Component
    public static class Factory {
        private final BeanFactory beanFactory;
        
        public Factory() {
            // Provide a default constructor so this class can be proxied by Weld when used in the legacy Wildfly web service.
            this(null);
        }
        
        public Factory(BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }
        
        @SuppressWarnings("unchecked")
        public TypeMetadataHelper createTypeMetadataHelper(AccumuloClient client, String metadataTableName, Set<Authorizations> auths,
                        boolean useTypeSubstitution) {
            if (beanFactory == null) {
                log.info("TypeMetadataHelper created without a beanFactory. This is fine for unit tests, but an error in production.");
                return new TypeMetadataHelper(Maps.newHashMap(), Collections.emptySet(), client, metadataTableName, auths, useTypeSubstitution);
            } else {
                Map<String,String> typeSubstitutions = (Map<String,String>) beanFactory.getBean("typeSubstitutions",
                                ResolvableType.forClassWithGenerics(Map.class, String.class, String.class).resolve());
                Set<Authorizations> allMetadataAuths = (Set<Authorizations>) beanFactory.getBean("allMetadataAuths",
                                ResolvableType.forClassWithGenerics(Set.class, Authorizations.class).resolve());
                return new TypeMetadataHelper(typeSubstitutions, allMetadataAuths, client, metadataTableName, auths, useTypeSubstitution);
            }
        }
    }
}
