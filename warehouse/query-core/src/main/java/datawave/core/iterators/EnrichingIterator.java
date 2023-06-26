package datawave.core.iterators;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.query.enrich.DataEnricher;

import datawave.query.Constants;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;

/**
 * <p>
 * An iterator that passes keys through a configured list of {@link datawave.query.enrich.DataEnricher}s, like the {@link datawave.query.enrich.EnrichingMaster}
 * .
 * </p>
 *
 * <p>
 * This iterator should be configured at a higher priority than the sub iterators so that all of the key/values returned by your query iterators are enriched
 * before being returned to the user.
 * </p>
 *
 *
 * @see datawave.query.enrich.EnrichingMaster
 *
 */
public class EnrichingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static final Logger log = Logger.getLogger(EnrichingIterator.class);
    public static final String ENRICHMENT_CLASSES = "enrichment.classes";
    public static final String QUERY = "query";
    public static final String SUB_ITERATOR_CLASS = "sub.iterator.class";
    public static final String UNEVALUATED_FIELDS = "enriching.unevaluated.fields";

    private List<DataEnricher> enrichers = null;
    private Key topKey = null;
    private Value topValue = null;
    private SortedKeyValueIterator<Key,Value> subIter = null;

    public EnrichingIterator() {}

    public EnrichingIterator(EnrichingIterator iter, IteratorEnvironment env) {
        this.subIter = iter.subIter;
        this.enrichers = iter.enrichers;
        this.topKey = iter.topKey;
        this.topValue = iter.topValue;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(ENRICHMENT_CLASSES, "'" + Constants.PARAM_VALUE_SEP + "' separated list of classes to use to enrich the data");
        options.put(QUERY, "The query that was submitted");
        options.put(SUB_ITERATOR_CLASS, "The class name of the iterator that lives below the Enriching Iterator.");
        options.put(UNEVALUATED_FIELDS, "'" + Constants.PARAM_VALUE_SEP + "' separated list of fields that do not exist in the event.");
        return new IteratorOptions(getClass().getSimpleName(), "Enriches the data passing through the iterator", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(ENRICHMENT_CLASSES) && options.containsKey(QUERY) && options.containsKey(SUB_ITERATOR_CLASS)
                        && options.containsKey(UNEVALUATED_FIELDS);

    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IOException("Invalid options");
        }

        this.enrichers = new ArrayList<>();

        String query = options.get(QUERY);
        String fields = options.get(UNEVALUATED_FIELDS);
        String classes = options.get(ENRICHMENT_CLASSES);
        String subClass = options.get(SUB_ITERATOR_CLASS);
        String[] classNames = StringUtils.split(classes, Constants.PARAM_VALUE_SEP);
        String[] unevaluatedFields = StringUtils.split(fields, Constants.PARAM_VALUE_SEP);

        Map<String,Object> enricherOptions = new HashMap<>();
        enricherOptions.put("query", query);
        enricherOptions.put("unevaluatedFields", unevaluatedFields);

        for (String className : classNames) {
            try {
                Class<?> clz = Class.forName(className);

                if (!DataEnricher.class.isAssignableFrom(clz)) {
                    log.warn("Ignoring enrichment class '" + className + "' as it does not extend DataEnricher");
                    continue;
                }

                DataEnricher enricher = (DataEnricher) clz.getDeclaredConstructor().newInstance();
                try {
                    enricher.init(source.deepCopy(env), enricherOptions, env);
                } catch (ParseException e) {
                    log.error("Could not instantiate " + className + ".", e);
                    continue;
                }

                enrichers.add(enricher);
            } catch (ClassNotFoundException e) {
                log.error("ClassNotFoundException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                log.error("InstantiationException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            } catch (IllegalAccessException e) {
                log.error("IllegalAccessException when trying to instantiate the enrichers.", e);
                log.error("Ignoring provided enricher class name: " + className);
            }
        }

        try {
            @SuppressWarnings("unchecked")
            Class<SortedKeyValueIterator<Key,Value>> iterClz = (Class<SortedKeyValueIterator<Key,Value>>) Class.forName(subClass);
            subIter = iterClz.newInstance();
        } catch (ClassNotFoundException e) {
            log.error("ClassNotFoundException when trying to instantiate the sub iterator.", e);
        } catch (InstantiationException e) {
            log.error("InstantiationException when trying to instantiate the sub iterator.", e);
        } catch (IllegalAccessException e) {
            log.error("IllegalAccessException when trying to instantiate the sub iterator.", e);
        }

        if (subIter == null) {
            return;
        } else {
            subIter.init(source.deepCopy(env), options, env);
        }

        if (this.subIter.hasTop()) {
            this.topKey = this.subIter.getTopKey();
            this.topValue = this.subIter.getTopValue();

            this.enrich();
        }
    }

    @Override
    public boolean hasTop() {
        return this.subIter != null && this.subIter.hasTop();
    }

    @Override
    public void next() throws IOException {
        if (this.subIter == null) {
            return;
        }

        this.subIter.next();

        if (this.subIter.hasTop()) {
            this.topKey = this.subIter.getTopKey();
            this.topValue = this.subIter.getTopValue();

            if (this.topKey != null) {
                this.enrich();
            }
        } else {
            this.topKey = null;
            this.topValue = null;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (this.subIter == null) {
            return;
        }

        this.subIter.seek(range, columnFamilies, inclusive);

        if (this.subIter.hasTop()) {
            this.topKey = this.subIter.getTopKey();
            this.topValue = this.subIter.getTopValue();

            if (this.topKey != null) {
                this.enrich();
            }
        } else {
            this.topKey = null;
            this.topValue = null;
        }

    }

    @Override
    public Key getTopKey() {
        return this.topKey;
    }

    @Override
    public Value getTopValue() {
        return this.topValue;
    }

    private void enrich() {
        Key key = this.topKey;
        Value value = this.topValue;
        boolean valid;

        for (DataEnricher enricher : this.enrichers) {
            valid = enricher.enrich(key, value);

            if (!valid) {
                this.topKey = null;
                this.topValue = null;

                return;
            }
        }

        this.topKey = key;
        this.topValue = value;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new EnrichingIterator(this, env);
    }
}
