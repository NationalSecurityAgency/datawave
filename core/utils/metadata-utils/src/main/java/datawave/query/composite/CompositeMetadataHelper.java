package datawave.query.composite;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

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
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import datawave.data.ColumnFamilyConstants;
import datawave.security.util.ScannerHelper;

@EnableCaching
@Component("compositeMetadataHelper")
@Scope("prototype")
public class CompositeMetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(CompositeMetadataHelper.class);
    
    public static final String transitionDateFormat = "yyyyMMdd HHmmss.SSS";
    public static final String NULL_BYTE = "\0";
    
    protected final List<Text> metadataCompositeColfs = Arrays.asList(ColumnFamilyConstants.COLF_CI, ColumnFamilyConstants.COLF_CITD,
                    ColumnFamilyConstants.COLF_CISEP);
    
    protected final AccumuloClient accumuloClient;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    
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
    public CompositeMetadataHelper(AccumuloClient client, String metadataTableName, Set<Authorizations> auths) {
        Preconditions.checkNotNull(client, "A valid AccumuloClient is required by CompositeMetadataHelper");
        this.accumuloClient = client;
        
        Preconditions.checkNotNull(metadataTableName, "The metadata table name is required by CompositeMetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Accumulo scan Authorizations are required by CompositeMetadataHelper");
        this.auths = auths;
        
        if (log.isTraceEnabled()) {
            log.trace("Constructor  connector: " + accumuloClient.getClass().getCanonicalName() + " with auths: " + auths + " and metadata table name: "
                            + metadataTableName);
        }
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    @Cacheable(value = "getCompositeMetadata", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        log.debug("cache fault for getCompositeMetadata(" + this.auths + "," + this.metadataTableName + ")");
        return this.getCompositeMetadata(null);
    }
    
    @Cacheable(value = "getCompositeMetadata", key = "{#root.target.auths,#root.target.metadataTableName,#datatypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public CompositeMetadata getCompositeMetadata(Set<String> datatypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeMetadata(" + this.auths + "," + this.metadataTableName + "," + datatypeFilter + ")");
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        
        SimpleDateFormat dateFormat = new SimpleDateFormat(transitionDateFormat);
        
        // Scanner to the provided metadata table
        Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths);
        
        Range range = new Range();
        bs.setRange(range);
        
        // Fetch all the column
        for (Text colf : metadataCompositeColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        for (Entry<Key,Value> entry : bs) {
            Text colFam = entry.getKey().getColumnFamily();
            
            String colq = entry.getKey().getColumnQualifier().toString();
            int idx = colq.indexOf(NULL_BYTE);
            String type = colq.substring(0, idx); // this is the datatype
            
            if (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(type)) {
                String fieldName = entry.getKey().getRow().toString();
                if (colFam.equals(ColumnFamilyConstants.COLF_CITD)) {
                    if (null != entry.getKey().getColumnQualifier()) {
                        if (idx != -1) {
                            try {
                                Date transitionDate = dateFormat.parse(colq.substring(idx + 1));
                                compositeMetadata.addCompositeTransitionDateByType(type, fieldName, transitionDate);
                            } catch (ParseException e) {
                                log.trace("Unable to parse composite field transition date", e);
                            }
                        } else {
                            log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                        }
                    } else {
                        log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
                    }
                } else if (colFam.equals(ColumnFamilyConstants.COLF_CI)) {
                    // Get the column qualifier from the key. It contains the datatype
                    // and composite name,idx
                    if (null != entry.getKey().getColumnQualifier()) {
                        if (idx != -1) {
                            String[] componentFields = colq.substring(idx + 1).split(",");
                            compositeMetadata.setCompositeFieldMappingByType(type, fieldName, Arrays.asList(componentFields));
                        } else {
                            log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey());
                        }
                    } else {
                        log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey());
                    }
                } else if (colFam.equals(ColumnFamilyConstants.COLF_CISEP)) {
                    if (null != entry.getKey().getColumnQualifier()) {
                        if (idx != -1) {
                            String separator = colq.substring(idx + 1);
                            compositeMetadata.addCompositeFieldSeparatorByType(type, fieldName, separator);
                        } else {
                            log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey().toString());
                        }
                    } else {
                        log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey().toString());
                    }
                }
            }
        }
        
        bs.close();
        
        return compositeMetadata;
    }
}
