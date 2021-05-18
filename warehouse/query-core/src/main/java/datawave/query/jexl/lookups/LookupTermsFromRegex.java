package datawave.query.jexl.lookups;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.TimeoutIterator;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.BatchResource;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class LookupTermsFromRegex extends RegexIndexLookup {
    private static final Logger log = Logger.getLogger(LookupTermsFromRegex.class);
    
    protected MetadataHelper helperRef = null;
    protected Set<String> fields;
    protected Set<String> reverseFields;
    protected final Set<String> patterns;
    protected boolean unfieldedLookup = false;
    
    public LookupTermsFromRegex(Set<String> fields, Set<String> reverseFields, Set<String> patterns, MetadataHelper helperRef, boolean unfieldedLookup) {
        setMetadataHelper(helperRef);
        this.patterns = patterns;
        this.fields = fields;
        this.reverseFields = reverseFields;
        this.unfieldedLookup = unfieldedLookup;
    }
    
    public LookupTermsFromRegex(String fieldName, Set<String> patterns, MetadataHelper metadata) {
        this(Collections.singleton(fieldName), Collections.singleton(fieldName), patterns, metadata, false);
    }
    
    public void setMetadataHelper(MetadataHelper helper) {
        this.helperRef = helper;
    }
    
    @Override
    public IndexLookupMap lookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, long maxLookupConfigured) {
        IndexLookupMap fieldsToValues = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
        fieldsToValues.setPatterns(patterns);
        
        Multimap<String,Range> forwardMap = ArrayListMultimap.create(), reverseMap = ArrayListMultimap.create();
        
        // Loop over all the patterns, classifying them as forward or reverse index satisfiable
        Iterator<Entry<Key,Value>> iter = Collections.emptyIterator();
        
        Collection<ScannerSession> sessions = Lists.newArrayList();
        
        ScannerSession bs = null;
        
        IteratorSetting fairnessIterator = null;
        if (maxLookupConfigured > 0) {
            /**
             * The fairness iterator solves the problem whereby we have runaway iterators as a result of an evaluation that never finds anything
             */
            fairnessIterator = new IteratorSetting(1, TimeoutIterator.class);
            
            long maxTime = (long) (maxLookupConfigured * 1.25);
            fairnessIterator.addOption(TimeoutIterator.MAX_SESSION_TIME, Long.valueOf(maxTime).toString());
        }
        
        for (String pattern : patterns) {
            if (!isAcceptedPattern(pattern)) {
                PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.IGNORE_PATTERN_FOR_INDEX_LOOKUP,
                                MessageFormat.format("Pattern: {0}", pattern));
                log.debug(qe);
                throw new DoNotPerformOptimizedQueryException(qe);
            }
            
            ShardIndexQueryTableStaticMethods.RefactoredRangeDescription rangeDescription = null;
            try {
                rangeDescription = ShardIndexQueryTableStaticMethods.getRegexRange(null, pattern, config.getFullTableScanEnabled(), helperRef, config);
            } catch (IllegalArgumentException e) {
                log.debug("Ignoring pattern that was not capable of being looked up in the index: " + pattern, e);
                continue;
            } catch (JavaRegexParseException e) {
                log.debug("Ignoring pattern that was not capable of being looked up in the index: " + pattern, e);
                continue;
            } catch (TableNotFoundException e) {
                log.error(e);
                throw new DatawaveFatalQueryException(e);
            } catch (ExecutionException e) {
                throw new DatawaveFatalQueryException(e);
            }
            if (log.isTraceEnabled()) {
                log.trace("Adding pattern " + pattern);
                log.trace("Adding pattern " + rangeDescription);
            }
            if (rangeDescription.isForReverseIndex) {
                
                reverseMap.put(pattern, rangeDescription.range);
            } else {
                forwardMap.put(pattern, rangeDescription.range);
            }
        }
        
        if (!fields.isEmpty() && !forwardMap.isEmpty()) {
            for (String key : forwardMap.keySet()) {
                Collection<Range> ranges = forwardMap.get(key);
                try {
                    bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getIndexTableName(), ranges,
                                    Collections.emptySet(), Collections.singleton(key), false, true);
                    
                    bs.setResourceClass(BatchResource.class);
                } catch (Exception e) {
                    throw new DatawaveFatalQueryException(e);
                }
                SessionOptions opts = bs.getOptions();
                if (null != fairnessIterator) {
                    opts.addScanIterator(fairnessIterator);
                    
                    IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
                    opts.addScanIterator(cfg);
                    
                }
                
                for (String field : fields) {
                    opts.fetchColumnFamily(new Text(field));
                }
                
                sessions.add(bs);
                iter = Iterators.concat(iter, bs);
                
            }
            
            try {
                timedScan(iter, fieldsToValues, config, unfieldedLookup, fields, false, maxLookupConfigured, log);
            } finally {
                for (ScannerSession sesh : sessions) {
                    scannerFactory.close(sesh);
                }
            }
        }
        
        sessions.clear();
        if (!reverseFields.isEmpty() && !reverseMap.isEmpty()) {
            for (String key : reverseMap.keySet()) {
                Collection<Range> ranges = reverseMap.get(key);
                if (log.isTraceEnabled()) {
                    log.trace("adding " + ranges + " for reverse");
                }
                try {
                    
                    bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getReverseIndexTableName(), ranges,
                                    Collections.emptySet(), Collections.singleton(key), true, true);
                    
                    bs.setResourceClass(BatchResource.class);
                } catch (Exception e) {
                    throw new DatawaveFatalQueryException(e);
                }
                SessionOptions opts = bs.getOptions();
                if (null != fairnessIterator) {
                    opts.addScanIterator(fairnessIterator);
                    if (null != fairnessIterator) {
                        IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
                        opts.addScanIterator(cfg);
                    }
                }
                
                for (String field : reverseFields) {
                    opts.fetchColumnFamily(new Text(field));
                }
                
                sessions.add(bs);
                iter = Iterators.concat(iter, bs);
                
            }
            try {
                timedScan(iter, fieldsToValues, config, unfieldedLookup, reverseFields, true, maxLookupConfigured, log);
            } finally {
                for (ScannerSession sesh : sessions) {
                    scannerFactory.close(sesh);
                }
            }
        }
        
        return fieldsToValues;
    }
    
    @Override
    protected Callable<Boolean> createTimedCallable(final Iterator<Entry<Key,Value>> iter, final IndexLookupMap fieldsToValues, ShardQueryConfiguration config,
                    final boolean unfieldedLookup, final Set<String> fields, final boolean isReverse, long timeout) {
        final Set<String> myDatatypeFilter = config.getDatatypeFilter();
        return () -> {
            Text holder = new Text();
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Do we have next? " + iter.hasNext());
                    
                }
                while (iter.hasNext()) {
                    
                    Entry<Key,Value> entry = iter.next();
                    
                    if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                        throw new Exception("Exceeded fair threshold");
                    }
                    
                    Key topKey = entry.getKey();
                    
                    if (log.isTraceEnabled()) {
                        log.trace("Foward Index entry: " + entry.getKey());
                    }
                    
                    // Get the column qualifier from the key. It contains the datatype and normalizer class
                    if (null != topKey.getColumnQualifier()) {
                        String colq = topKey.getColumnQualifier().toString();
                        int idx = colq.indexOf(Constants.NULL);
                        
                        if (idx != -1) {
                            String type = colq.substring(idx + 1);
                            
                            // If types are specified and this type is not in the list, skip it.
                            if (null != myDatatypeFilter && !myDatatypeFilter.isEmpty() && !myDatatypeFilter.contains(type)) {
                                
                                if (log.isTraceEnabled())
                                    log.trace(myDatatypeFilter + " does not contain " + type);
                                continue;
                            }
                            
                            topKey.getRow(holder);
                            String term;
                            if (!isReverse)
                                term = holder.toString();
                            else
                                term = (new StringBuilder(holder.toString())).reverse().toString();
                            
                            topKey.getColumnFamily(holder);
                            String field = holder.toString();
                            
                            // We are only returning a mapping of field value to field name, no need to
                            // determine cardinality and such at this point.
                            fieldsToValues.put(field, term);
                            // conditional states that if we exceed the key threshold OR field name is not null and we've exceeded
                            // the value threshold for that field name ( in the case where we have a fielded lookup ).
                            if (fieldsToValues.isKeyThresholdExceeded() || (fields.size() == 1 && fieldsToValues.get(field).isThresholdExceeded())) {
                                if (log.isTraceEnabled())
                                    log.trace("We've passed term expansion threshold");
                                return true;
                            }
                        }
                    }
                }
                
            } catch (Exception e) {
                log.info("Failed or Timed out expanding regex: " + e.getMessage());
                if (log.isDebugEnabled())
                    log.debug("Failed or Timed out " + e);
                // Only if not doing an unfielded lookup should we mark all fields as having an exceeded threshold
                if (!unfieldedLookup) {
                    for (String field : fields) {
                        if (log.isTraceEnabled()) {
                            log.trace("field is " + field);
                            log.trace("field is " + (null == fieldsToValues));
                        }
                        fieldsToValues.put(field, "");
                        fieldsToValues.get(field).setThresholdExceeded();
                    }
                } else
                    fieldsToValues.setKeyThresholdExceeded();
                return false;
            }
            
            return true;
        };
    }
    
    public static class TooMuchExpansionException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
