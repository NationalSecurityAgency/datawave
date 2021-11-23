package datawave.experimental.util;

import com.google.common.collect.Sets;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.QueryOptions;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Collection of static utility methods for extracting relevant info from a {@link ScannerChunk}
 */
public class ScannerChunkUtil {
    
    public static Range rangeFromChunk(ScannerChunk chunk) {
        Collection<Range> ranges = chunk.getRanges();
        if (ranges.size() > 1) {
            throw new IllegalArgumentException("Attempted to run query with " + ranges.size() + ", expected 1 range.");
        }
        return chunk.getRanges().iterator().next();
    }
    
    public static String scanIdFromChunk(ScannerChunk chunk) {
        IteratorSetting setting = chunk.getOptions().getIterators().iterator().next();
        
        Map<String,String> optionsMap = setting.getOptions();
        String queryId = optionsMap.get(QueryOptions.QUERY_ID);
        
        Range range = rangeFromChunk(chunk);
        String shard = range.getStartKey().getRow().toString();
        String uid = null;
        
        String cf = range.getStartKey().getColumnFamily().toString();
        if (!cf.isEmpty()) {
            int index = cf.indexOf('\u0000');
            uid = cf.substring(index + 1);
        }
        
        if (uid == null) {
            // shard range
            return queryId + "::" + shard;
        } else {
            // doc range
            return queryId + "::" + shard + "::" + uid;
        }
    }
    
    public static Set<String> indexedFieldsFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        String s = optionsMap.get(QueryOptions.INDEXED_FIELDS);
        return Sets.newHashSet(s.split(","));
    }
    
    public static Set<String> indexOnlyFieldsFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        String s = optionsMap.get(QueryOptions.INDEX_ONLY_FIELDS);
        return Sets.newHashSet(s.split(","));
    }
    
    public static Set<String> termFrequencyFieldsFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        if (optionsMap.containsKey(QueryOptions.TERM_FREQUENCY_FIELDS)) {
            String s = optionsMap.get(QueryOptions.TERM_FREQUENCY_FIELDS);
            return Sets.newHashSet(s.split(","));
        } else {
            return Collections.emptySet();
        }
    }
    
    public static boolean tfRequiredFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        return Boolean.parseBoolean(optionsMap.getOrDefault(QueryOptions.TERM_FREQUENCIES_REQUIRED, "false"));
    }
    
    public static String queryFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        return optionsMap.get(QueryOptions.QUERY);
    }
    
    public static Authorizations authsFromChunk(ScannerChunk chunk) {
        return chunk.getOptions().getConfiguration().getAuthorizations().iterator().next();
    }
    
    public static CompositeMetadata compositeMetadataFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        if (optionsMap.containsKey(QueryOptions.COMPOSITE_METADATA)) {
            String compositeMetadataString = optionsMap.get(QueryOptions.COMPOSITE_METADATA);
            if (compositeMetadataString != null && !compositeMetadataString.isEmpty()) {
                return CompositeMetadata.fromBytes(java.util.Base64.getDecoder().decode(compositeMetadataString));
            }
        }
        return null;
    }
    
    public static TypeMetadata typeMetadataFromChunk(ScannerChunk chunk) {
        SessionOptions options = chunk.getOptions();
        IteratorSetting setting = options.getIterators().iterator().next();
        Map<String,String> optionsMap = setting.getOptions();
        if (optionsMap.containsKey(QueryOptions.TYPE_METADATA)) {
            String typeMetadataString = optionsMap.get(QueryOptions.TYPE_METADATA);
            return new TypeMetadata(typeMetadataString);
        }
        return null;
    }
    
}
