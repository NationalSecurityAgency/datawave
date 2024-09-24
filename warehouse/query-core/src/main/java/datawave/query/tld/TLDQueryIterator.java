package datawave.query.tld;

import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.getExpressionFilters;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.function.Equality;
import datawave.query.function.RangeProvider;
import datawave.query.function.TLDEquality;
import datawave.query.function.TLDRangeProvider;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.SourcedOptions;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.postprocessing.tf.TFFactory;
import datawave.query.postprocessing.tf.TermFrequencyConfig;
import datawave.query.predicate.ChainableEventDataQueryFilter;
import datawave.query.predicate.ConfiguredPredicate;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TLDEventDataFilter;
import datawave.query.predicate.TLDFieldIndexQueryFilter;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.util.StringUtils;

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
        super.getDocumentKey = GetStartKeyForRoot.instance();
        return success;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("TLDQueryIterator init()");
        }

        super.init(source, options, env);

        // Replace the fieldIndexKeyDataTypeFilter with a chain of "anded" index-filtering predicates.
        // If no other predicates are configured via the indexfiltering.classes property, the method
        // simply returns the existing fieldIndexKeyDataTypeFilter value. Otherwise, the returned value
        // contains an "anded" chain of newly configured predicates following the existing
        // fieldIndexKeyDataTypeFilter value (assuming it is defined with something other than the default
        // "ALWAYS_TRUE" KeyIdentity.Function).
        fieldIndexKeyDataTypeFilter = parseIndexFilteringChain(new SourcedOptions<>(source, env, options));

        disableIndexOnlyDocuments = false;
    }

    /**
     * Get a FieldIndexAggregator for the {@link TLDQueryIterator}
     *
     * @return a {@link TLDFieldIndexAggregator}
     */
    @Override
    public FieldIndexAggregator getFiAggregator() {
        if (fiAggregator == null) {
            fiAggregator = new TLDFieldIndexAggregator(getNonEventFields(), getFIEvaluationFilter(), getFiNextSeek());
        }
        return fiAggregator;
    }

    /**
     * Distinct from getEvaluation filter as the FI filter is used to prevent FI hits on nonEventFields that are not indexOnly fields
     *
     * @return an {@link EventDataQueryFilter}
     */
    protected EventDataQueryFilter getFIEvaluationFilter() {
        ChainableEventDataQueryFilter filterChain = new ChainableEventDataQueryFilter();
        // primary filter on the current filter
        filterChain.addFilter(getEvaluationFilter());
        // prevent anything that is not an index only field from being kept at the tld level, otherwise allow all
        filterChain.addFilter(new TLDFieldIndexQueryFilter(getIndexOnlyFields()));
        return filterChain;
    }

    @Override
    public EventDataQueryFilter getEvaluationFilter() {
        if (this.evaluationFilter == null && script != null) {

            AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
            Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);

            // setup an evaluation filter to avoid loading every single child key into the event
            this.evaluationFilter = new TLDEventDataFilter(script, getAllFields(), expressionFilters, useAllowListedFields ? allowListedFields : null,
                            useDisallowListedFields ? disallowListedFields : null, getEventFieldSeek(), getEventNextSeek(),
                            limitFieldsPreQueryEvaluation ? limitFieldsMap : Collections.emptyMap(), limitFieldsField, getNonEventFields());
        }
        return this.evaluationFilter != null ? evaluationFilter.clone() : null;
    }

    /**
     * In the TLD case replace the {@link QueryOptions#eventFilter} with an evaluation filter
     *
     * @return an evaluation filter
     */
    public EventDataQueryFilter getEventFilter() {
        return getEvaluationFilter();
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
                range = new Range(startKey, false, endKey, endKeyInclusive);
            }
        }

        super.seek(range, columnFamilies, inclusive);
    }

    @Override
    protected IteratorBuildingVisitor createIteratorBuildingVisitor(final Range documentRange, boolean isQueryFullySatisfied, boolean sortedUIDs)
                    throws MalformedURLException, ConfigException, InstantiationException, IllegalAccessException {
        return createIteratorBuildingVisitor(TLDIndexBuildingVisitor.class, documentRange, isQueryFullySatisfied, sortedUIDs)
                        .setIteratorBuilder(TLDIndexIteratorBuilder.class);
    }

    @Override
    protected Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> buildTfFunction(TermFrequencyConfig tfConfig) {
        tfConfig.setTld(true);
        return TFFactory.getFunction(tfConfig);
    }

    /**
     * Get a {@link TLDRangeProvider}
     *
     * @return a {@link TLDRangeProvider}
     */
    @Override
    public RangeProvider getRangeProvider() {
        if (rangeProvider == null) {
            rangeProvider = new TLDRangeProvider();
        }
        return rangeProvider;
    }

    /**
     * Get a {@link TLDEquality} implementation of an {@link Equality}
     *
     * @return a TLDEquality
     */
    @Override
    public Equality getEquality() {
        if (equality == null) {
            equality = new TLDEquality();
        }
        return equality;
    }
}
