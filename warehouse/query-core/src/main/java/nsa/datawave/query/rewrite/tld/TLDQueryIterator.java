package nsa.datawave.query.rewrite.tld;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import nsa.datawave.query.rewrite.planner.SeekingQueryPlanner;
import nsa.datawave.query.rewrite.predicate.EventDataQueryFilter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import nsa.datawave.query.rewrite.function.TLDEquality;
import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.QueryIterator;
import nsa.datawave.query.rewrite.iterator.SourcedOptions;
import nsa.datawave.query.rewrite.iterator.logic.IndexIterator;
import nsa.datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import nsa.datawave.query.rewrite.predicate.ConfiguredPredicate;
import nsa.datawave.query.rewrite.predicate.TLDEventDataFilter;
import nsa.datawave.util.StringUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This is a TLD (Top Level Document) QueryIterator implementation.
 */
public class TLDQueryIterator extends QueryIterator {
    private static final Logger log = Logger.getLogger(TLDQueryIterator.class);
    
    protected int maxFieldHitsBeforeSeek = -1;
    protected int maxKeysBeforeSeek = -1;
    
    public TLDQueryIterator() {}
    
    public TLDQueryIterator(TLDQueryIterator other, IteratorEnvironment env) {
        super(other, env);
    }
    
    @Override
    public TLDQueryIterator deepCopy(IteratorEnvironment env) {
        return new TLDQueryIterator(this, env);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean success = super.validateOptions(options);
        super.equality = new TLDEquality();
        super.getDocumentKey = GetStartKeyForRoot.instance();
        return success;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("TLDQueryIterator init()");
        }
        
        // extract SeekingQueryPlanner fields if available
        if (options.get(SeekingQueryPlanner.MAX_FIELD_HITS_BEFORE_SEEK) != null) {
            maxFieldHitsBeforeSeek = Integer.parseInt(options.get(SeekingQueryPlanner.MAX_FIELD_HITS_BEFORE_SEEK));
        }
        
        if (options.get(SeekingQueryPlanner.MAX_KEYS_BEFORE_SEEK) != null) {
            maxKeysBeforeSeek = Integer.parseInt(options.get(SeekingQueryPlanner.MAX_KEYS_BEFORE_SEEK));
        }
        
        super.init(source, options, env);
        
        super.fiAggregator = new TLDFieldIndexAggregator(getAllIndexOnlyFields(), getEvaluationFilter(), maxKeysBeforeSeek);
        
        // Replace the fieldIndexKeyDataTypeFilter with a chain of "anded" index-filtering predicates.
        // If no other predicates are configured via the indexfiltering.classes property, the method
        // simply returns the existing fieldIndexKeyDataTypeFilter value. Otherwise, the returned value
        // contains an "anded" chain of newly configured predicates following the existing
        // fieldIndexKeyDataTypeFilter value (assuming it is defined with something other than the default
        // "ALWAYS_TRUE" KeyIdentity.Function).
        fieldIndexKeyDataTypeFilter = parseIndexFilteringChain(new SourcedOptions<String,String>(source, env, options));
        
        disableIndexOnlyDocuments = false;
    }
    
    @Override
    public EventDataQueryFilter getEvaluationFilter() {
        if (this.evaluationFilter == null && script != null) {
            // setup an evaluation filter to avoid loading every single child key into the event
            this.evaluationFilter = new TLDEventDataFilter(script, typeMetadata, this.isDataQueryExpressionFilterEnabled(),
                            useWhiteListedFields ? whiteListedFields : null, useBlackListedFields ? blackListedFields : null, maxFieldHitsBeforeSeek,
                            maxKeysBeforeSeek, limitFieldsPreQueryEvaluation ? limitFieldsMap : Collections.EMPTY_MAP);
        }
        return this.evaluationFilter;
    }
    
    @Override
    protected NestedIterator<Key> getEventDataNestedIterator(SortedKeyValueIterator<Key,Value> source) {
        return new TLDEventDataScanNestedIterator(source, getEventEntryKeyDataTypeFilter());
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    private Predicate<Key> parseIndexFilteringChain(final Map<String,String> options) {
        // Create a list to gather up the predicates
        List<Predicate<Key>> predicates = Collections.emptyList();
        
        final String functions = (null != options) ? options.get(IndexIterator.INDEX_FILTERING_CLASSES) : StringUtils.EMPTY_STRING;
        if ((null != functions) && !functions.isEmpty()) {
            try {
                for (final String fClassName : StringUtils.splitIterable(functions, ',', true)) {
                    // Log it
                    if (log.isTraceEnabled()) {
                        log.trace("Configuring index-filtering class: " + fClassName);
                    }
                    
                    final Class<?> fClass = Class.forName(fClassName);
                    if (Predicate.class.isAssignableFrom(fClass)) {
                        // Create and configure the predicate
                        final Predicate p = (Predicate) fClass.newInstance();
                        if (p instanceof ConfiguredPredicate) {
                            ((ConfiguredPredicate) p).configure(options);
                        }
                        
                        // Initialize a mutable List instance and add the default filter, if defined
                        if (predicates.isEmpty()) {
                            predicates = new LinkedList<>();
                            final Predicate<Key> existingPredicate = fieldIndexKeyDataTypeFilter;
                            if ((null != existingPredicate) && (((Object) existingPredicate) != Predicates.alwaysTrue())) {
                                predicates.add(existingPredicate);
                            }
                        }
                        
                        // Add the newly instantiated predicate
                        predicates.add(p);
                    } else {
                        log.error(fClass + " is not a function or predicate. Postprocessing will not be performed.");
                        return fieldIndexKeyDataTypeFilter;
                    }
                }
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                log.error("Could not instantiate postprocessing chain!", e);
            }
        }
        
        // Assign the return value
        final Predicate<Key> predicate;
        if (!predicates.isEmpty()) {
            if (predicates.size() == 1) {
                predicate = predicates.get(0);
            } else {
                predicate = Predicates.and(predicates);
            }
        } else {
            predicate = fieldIndexKeyDataTypeFilter;
        }
        
        return predicate;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // when we are torn down and rebuilt, ensure the range is for the next top level document
        if (!range.isStartKeyInclusive()) {
            Key startKey = TLD.getNextParentKey(range.getStartKey());
            if (!startKey.equals(range.getStartKey())) {
                Key endKey = range.getEndKey();
                boolean endKeyInclusive = range.isEndKeyInclusive();
                // if the start key is outside of the range, then reset the end key to the next key
                if (range.afterEndKey(startKey)) {
                    endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
                    endKeyInclusive = false;
                }
                range = new Range(startKey, false, endKey, endKeyInclusive);
            }
        }
        
        super.seek(range, columnFamilies, inclusive);
        
    }
    
    @Override
    protected IteratorBuildingVisitor createIteratorBuildingVisitor(final Range documentRange, boolean isQueryFullySatisfied, boolean sortedUIDs)
                    throws MalformedURLException, ConfigException, InstantiationException, IllegalAccessException {
        return createIteratorBuildingVisitor(TLDIndexBuildingVisitor.class, documentRange, isQueryFullySatisfied, sortedUIDs).setIteratorBuilder(
                        TLDIndexIteratorBuilder.class);
    }
    
}
