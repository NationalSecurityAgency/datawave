package datawave.query.discovery;

import static com.google.common.collect.Sets.newTreeSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.core.iterators.GlobalIndexTermMatchingIterator;
import datawave.core.iterators.filter.GlobalIndexTermMatchingFilter;
import datawave.query.iterator.UniqueColumnFamilyIterator;
import datawave.query.jexl.LiteralRange;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A combination of {@link GlobalIndexTermMatchingIterator} and {@link GlobalIndexTermMatchingFilter} with the ability to consider column families.
 */
public class IndexMatchingIterator implements SortedKeyValueIterator<Key,Value> {
    public static final String CONF = "term.pattern.";
    public static final String REVERSE_INDEX = "reverse.index";
    public static final String RANGE = "term.range.";

    private static final Pair<Boolean,Optional<ImmutableSortedSet<String>>> ALL_FIELDS = Pair.with(Boolean.TRUE,
                    Optional.<ImmutableSortedSet<String>> absent());
    private static final Logger log = Logger.getLogger(IndexMatchingIterator.class);

    // configured options
    private Set<String> unfieldedLiterals;
    private Multimap<String,String> fieldedLiterals;
    private Set<Matcher> unfieldedMatchers;
    private Multimap<Matcher,String> fieldedMatchers;
    private Set<LiteralRange<String>> unfieldedRanges;
    private Multimap<String,LiteralRange<String>> fieldedRanges;
    private boolean reverseIndex;

    // involved w/ scanning keys and values
    private SortedKeyValueIterator<Key,Value> src;
    private Key topKey;
    private Value topValue;

    // used to skip ahead and preserve the original scan range
    private Range scanRange;
    private Collection<ByteSequence> scanCFs;
    private boolean scanInclusive;

    // cache of the set of fields that could match the previous term
    private Pair<String,Pair<Boolean,Optional<ImmutableSortedSet<String>>>> fieldsForLastTerm;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (env != null && !IteratorScope.scan.equals(env.getIteratorScope())) {
            throw new IOException("This is a scan time only iterator.");
        }

        src = source;

        Gson gson = gson();

        Configuration conf = gson.fromJson(options.get(CONF), Configuration.class);
        this.unfieldedLiterals = conf.getUnfieldedLiterals();
        this.fieldedLiterals = conf.getFieldedLiterals();
        this.unfieldedRanges = conf.getUnfieldedRanges();
        this.fieldedRanges = conf.getFieldedRanges();

        this.unfieldedMatchers = Sets.newIdentityHashSet();
        for (String pattern : conf.getUnfieldedPatterns()) {
            Matcher m = Pattern.compile(pattern).matcher("");
            this.unfieldedMatchers.add(m);
        }

        this.fieldedMatchers = HashMultimap.create();
        for (Entry<String,Collection<String>> pattern : conf.getFieldedPatterns().asMap().entrySet()) {
            Matcher m = Pattern.compile(pattern.getKey()).matcher("");
            this.fieldedMatchers.putAll(m, pattern.getValue());
        }

        if (options.containsKey(REVERSE_INDEX)) {
            reverseIndex = Boolean.parseBoolean(options.get(REVERSE_INDEX));
        } else {
            reverseIndex = false;
        }

        topKey = null;
        topValue = null;
    }

    @Override
    public boolean hasTop() {
        return topKey != null;
    }

    @Override
    public void next() throws IOException {
        Key lastTop = topKey;
        topKey = null;
        while (src.hasTop()) {
            Key key = src.getTopKey();

            /*
             * since we already validated a given row+colfam, if the last top has the same row+colfam, then this key is automagically valid.
             */
            if (lastTop != null && lastTop.equals(key, PartialKey.ROW_COLFAM)) {
                if (log.isDebugEnabled())
                    log.debug("Automatic match because of lastKey[" + lastTop + "]" + ", key[" + key + "]");
                propagateTop();
                return;
            }

            String row = stringify(key.getRowData());
            String term = (reverseIndex ? new StringBuilder().append(row).reverse().toString() : row);

            Pair<Boolean,Optional<ImmutableSortedSet<String>>> matches = fieldsFor(term);

            if (matches.getValue0()) {
                Optional<ImmutableSortedSet<String>> maybeFields = matches.getValue1();
                if (maybeFields.isPresent()) {
                    ImmutableSortedSet<String> fields = maybeFields.get();
                    String field = stringify(key.getColumnFamilyData());
                    String possibleMatch = fields.ceiling(field);
                    if (log.isDebugEnabled()) {
                        log.debug("Field is " + field + " possible match is " + possibleMatch);
                    }
                    if (field.equals(possibleMatch)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Returning");
                        }
                        propagateTop();
                        return;
                    } else {
                        Key next = possibleMatch == null ? key.followingKey(PartialKey.ROW) : new Key(row, possibleMatch);

                        UniqueColumnFamilyIterator.moveTo(next, src, scanRange, scanCFs, scanInclusive);
                        continue;
                    }
                } else {
                    // if there weren't any fields, then we can simply set our top k/v and return
                    propagateTop();
                    return;
                }
            } else {
                // we didn't find a match for the term
                UniqueColumnFamilyIterator.moveTo(key.followingKey(PartialKey.ROW), src, scanRange, scanCFs, scanInclusive);
            }
        }
    }

    /*
     * Macro-like function to set the top and advance the source.
     */
    private void propagateTop() throws IOException {
        topKey = new Key(src.getTopKey());
        topValue = src.getTopValue();
        src.next();
    }

    /*
     * Checks the cached value for the term. If it matches, we just return the cached value, otherwise we compute the fields for the term, set it to the last
     * cached value and return.
     */
    private Pair<Boolean,Optional<ImmutableSortedSet<String>>> fieldsFor(String term) {
        if (fieldsForLastTerm == null || !fieldsForLastTerm.getValue0().equals(term)) {
            if (log.isDebugEnabled())
                log.debug("Need to calculate fields for term[" + term + "]");
            fieldsForLastTerm = Pair.with(term, computeFieldsFor(term));
        }
        if (log.isDebugEnabled())
            log.debug("Returning " + fieldsForLastTerm.getValue1());
        return fieldsForLastTerm.getValue1();
    }

    /*
     * Checks to see if a term matches a configured literal or pattern and returns the fields that match.
     *
     * The return value is a pair of boolean and an optional set.
     *
     * - If the boolean value is true, that means the term matched a literal or pattern. - if the set is present, then we have a set of fields we want to filter
     * column families on - if the set is not present, then we can allow all column families/fields - therefore, the set should never present and empty - If the
     * boolean value is false, then the term did not match and the set should be ignored
     */
    private Pair<Boolean,Optional<ImmutableSortedSet<String>>> computeFieldsFor(String term) {
        // check the unfielded literals and patterns first because we can bail early
        if (this.unfieldedLiterals.contains(term)) {
            if (log.isDebugEnabled())
                log.debug("\"" + term + "\" matched an unfielded literal-- returning ALL_FIELDS.");
            return ALL_FIELDS;
        }
        for (Matcher matcher : this.unfieldedMatchers) {
            matcher.reset(term);
            if (matcher.matches()) {
                if (log.isDebugEnabled())
                    log.debug("\"" + term + "\" matched an unfielded pattern-- returning ALL_FIELDS.");
                return ALL_FIELDS;
            }
        }
        for (LiteralRange<String> range : this.unfieldedRanges) {
            boolean matches = range.contains(term);
            if (matches) {
                if (log.isDebugEnabled())
                    log.debug("\"" + term + "\" matched an unfielded range-- returning ALL_FIELDS.");
                return ALL_FIELDS;
            }
        }

        ImmutableSortedSet.Builder<String> fields = ImmutableSortedSet.naturalOrder();
        fields.addAll(fieldedLiterals.get(term));

        for (Entry<Matcher,Collection<String>> e : fieldedMatchers.asMap().entrySet()) {
            Matcher m = e.getKey();
            m.reset(term);
            if (m.matches()) {
                Collection<String> matcherFields = e.getValue();
                if (matcherFields.isEmpty()) {
                    if (log.isDebugEnabled())
                        log.debug("\"" + term + "\" had a matcher that had no fields. We shouldn't be here, but we're returning ALL_FIELDS.");
                    return ALL_FIELDS;
                } else {
                    fields.addAll(matcherFields);
                }
            }
        }

        for (Entry<String,Collection<LiteralRange<String>>> r : fieldedRanges.asMap().entrySet()) {
            for (LiteralRange<String> range : r.getValue()) {
                if (range.contains(term)) {
                    fields.add(r.getKey());
                    break;
                }
            }
        }

        ImmutableSortedSet<String> fieldSet = fields.build();

        Pair<Boolean,Optional<ImmutableSortedSet<String>>> p = Pair.with(!fieldSet.isEmpty(), Optional.of(fieldSet));
        if (log.isDebugEnabled())
            log.debug("For \"" + term + "\" returning " + p);
        return p;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.scanRange = range;
        this.scanCFs = columnFamilies;
        this.scanInclusive = inclusive;
        src.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public IndexMatchingIterator deepCopy(IteratorEnvironment env) {
        IndexMatchingIterator i = new IndexMatchingIterator();
        i.src = src.deepCopy(env);
        i.unfieldedLiterals = this.unfieldedLiterals;
        i.fieldedLiterals = this.fieldedLiterals;
        i.unfieldedMatchers = this.unfieldedMatchers;
        i.fieldedMatchers = this.fieldedMatchers;
        return i;
    }

    /**
     * Converts a byte sequence backed by a byte array into a String using the default encoding of the JVM.
     *
     * @param bs
     *            the byte sequence
     * @return the string form of the bytes
     */
    public static String stringify(ByteSequence bs) {
        Preconditions.checkArgument(bs.isBackedByArray(), "Received byte sequence that isn't backed by an array!");
        return new String(bs.getBackingArray(), bs.offset(), bs.length());
    }

    public static class Configuration {

        private Set<String> unfieldedLiterals = newTreeSet(), unfieldedPatterns = newTreeSet();
        private Multimap<String,String> fieldedLiterals = TreeMultimap.create(), fieldedPatterns = TreeMultimap.create();
        private Set<LiteralRange<String>> unfieldedRanges = new TreeSet();
        private Multimap<String,LiteralRange<String>> fieldedRanges = TreeMultimap.create();

        public boolean addLiteral(String literal) {
            return unfieldedLiterals.add(literal);
        }

        public boolean addLiteral(String literal, String field) {
            return fieldedLiterals.put(literal, field);
        }

        public boolean addPattern(String pattern) {
            return unfieldedPatterns.add(pattern);
        }

        public boolean addPattern(String pattern, String field) {
            return fieldedPatterns.put(pattern, field);
        }

        public boolean addRange(LiteralRange<String> range) {
            return unfieldedRanges.add(range);
        }

        public boolean addRange(LiteralRange<String> range, String field) {
            return fieldedRanges.put(field, range);
        }

        public Set<String> getUnfieldedLiterals() {
            return unfieldedLiterals;
        }

        public Set<String> getUnfieldedPatterns() {
            return unfieldedPatterns;
        }

        public Set<LiteralRange<String>> getUnfieldedRanges() {
            return unfieldedRanges;
        }

        public Multimap<String,String> getFieldedLiterals() {
            return fieldedLiterals;
        }

        public Multimap<String,String> getFieldedPatterns() {
            return fieldedPatterns;
        }

        public Multimap<String,LiteralRange<String>> getFieldedRanges() {
            return fieldedRanges;
        }

        @Override
        public String toString() {
            return "Configuration [unfieldedLiterals=" + unfieldedLiterals + ", unfieldedPatterns=" + unfieldedPatterns + ", unfieldedRanges=" + unfieldedRanges
                            + ", fieldedLiterals=" + fieldedLiterals + ", fieldedPatterns=" + fieldedPatterns + ", fieldedRanges=" + fieldedRanges + "]";
        }
    }

    public static Gson gson() {
        return new GsonBuilder().registerTypeAdapter(MultimapType.get(), new MultimapSerializer())
                        .registerTypeAdapter(LiteralRangeMultimapType.get(), new LiteralRangeMultimapSerializer())
                        .registerTypeAdapter(LiteralRangeType.get(), new LiteralRangeSerializer()).create();
    }

}
