package datawave.query.jexl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.iterators.filter.ageoff.FilterRule;
import datawave.query.Constants;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.util.StringUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Jexl Masking rule will provide a rule based upon JEXL 'queries'
 */
public class JexlRule extends AppliedRule {
    
    /**
     * Internal source
     */
    protected QueryIterator queryIter = null;
    
    protected boolean isApplied;
    
    private HashMap<String,String> iterOptions;
    
    protected IteratorEnvironment environment;
    
    private static final Logger log = Logger.getLogger(JexlRule.class);
    
    @Override
    public void init(FilterOptions options) {
        init(options, null);
    }
    
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        
        isApplied = false;
        
        iterOptions = Maps.newHashMap();
        iterOptions.put(QueryOptions.DISABLE_EVALUATION, "false");
        iterOptions.put(QueryOptions.QUERY, options.getOption("query"));
        iterOptions.put(QueryOptions.REDUCED_RESPONSE, "false");
        iterOptions.put(Constants.RETURN_TYPE, "kryo");
        iterOptions.put(QueryOptions.FULL_TABLE_SCAN_ONLY, "true");
        iterOptions.put(QueryOptions.FILTER_MASKED_VALUES, "true");
        iterOptions.put(QueryOptions.INCLUDE_DATATYPE, "true");
        iterOptions.put(QueryOptions.INDEX_ONLY_FIELDS, "");
        iterOptions.put(QueryOptions.INDEXED_FIELDS, "");
        iterOptions.put(QueryOptions.START_TIME, "0");
        iterOptions.put(QueryOptions.END_TIME, Long.toString(Long.MAX_VALUE));
        iterOptions.put(QueryOptions.POSTPROCESSING_CLASSES, "");
        iterOptions.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "false");
        iterOptions.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        iterOptions.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "false");
    }
    
    @Override
    public FilterRule decorate(Object decorate) {
        if (decorate instanceof IteratorEnvironment) {
            this.environment = (IteratorEnvironment) decorate;
        }
        
        return this;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.iterators.filter.ageoff.AppliedRule#accept(datawave.iterators.filter.ageoff.AgeOffPeriod, org.apache.accumulo.core.data.Key,
     * org.apache.accumulo.core.data.Value)
     */
    @Override
    public boolean accept(final SortedKeyValueIterator<Key,Value> iter) {
        // quick check to ensure that we have a document, if we do not, just
        // let it pass through
        if (null == iter || !iter.hasTop()) {
            isApplied = true;
            if (log.isDebugEnabled())
                log.debug("Returning false immediately as their is no top key in the source");
            
            return false;
        }
        if (isDocument(iter.getTopKey())) {
            isApplied = true;
            if (queryIter == null) {
                queryIter = new QueryIterator();
                try {
                    queryIter.init(iter.deepCopy(environment), iterOptions, environment);
                } catch (IOException e) {
                    log.debug("Failed to initialize queryIter with provided query", e);
                    return false;
                }
            }
            try {
                
                Key topKey = iter.getTopKey();
                if (log.isDebugEnabled())
                    log.debug(topKey);
                queryIter.seek(new Range(new Key(topKey.getRow(), topKey.getColumnFamily()), true, topKey.followingKey(PartialKey.ROW_COLFAM), false),
                                Collections.emptyList(), false);
                
            } catch (IOException e) {
                log.error(e);
                // review
                return false;
            }
            
            isApplied = true;
            // / means that we successfully matched the document, right?
            // amirite?
            if (queryIter.hasTop()) {
                if (log.isDebugEnabled())
                    log.debug(queryIter.getTopKey());
                return true;
            } else {
                if (log.isDebugEnabled())
                    log.debug("has no top ");
            }
            
            return false;
            
        } else {
            log.debug("false ");
            isApplied = false;
            return false;
        }
    }
    
    /**
     * @param topKey
     *            the top key
     * @return if this is a document
     */
    private static boolean isDocument(Key topKey) {
        String[] cfSplit = StringUtils.split(topKey.getColumnFamily().toString(), "\0");
        String[] cqSplit = StringUtils.split(topKey.getColumnQualifier().toString(), "\0");
        if (cfSplit.length == 2 && cqSplit.length == 2)
            return true;
        else {
            if (log.isDebugEnabled())
                log.debug(topKey);
        }
        
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.iterators.filter.ageoff.AppliedRule#isFilterRuleApplied()
     */
    @Override
    public boolean isFilterRuleApplied() {
        return isApplied;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.iterators.filter.ageoff.AppliedRule#accept(datawave.iterators.filter.ageoff.AgeOffPeriod, org.apache.accumulo.core.data.Key,
     * org.apache.accumulo.core.data.Value)
     */
    @Override
    public boolean accept(AgeOffPeriod arg0, Key arg1, Value arg2) {
        // accept method is not needed, here
        return false;
    }
    
}
