package datawave.query.predicate;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import datawave.query.data.parsers.EventKey;
import datawave.query.jexl.JexlASTHelper;

/**
 * Inclusive filter that ensures only event keys which match the set of fields to retain are kept for evaluation.
 * <p>
 * The fields to retain are built from query fields and user-specified <code>return.fields</code>
 * <p>
 * This filter only operates on event keys.
 */
public class EventDataQueryFieldFilter implements EventDataQueryFilter {

    private Key document = null;
    // the number of times next is called before issuing a seek
    private int maxNextCount = -1;
    // track the number of times next is called on the same field
    private int nextCount;
    // track the current field
    private String currentField = null;

    // the set of fields to retain
    private TreeSet<String> fields;
    private final EventKey parser;

    /**
     * Default constructor
     */
    public EventDataQueryFieldFilter() {
        this.parser = new EventKey();
    }

    /**
     * Copy constructor used by the {@link #clone()} method
     *
     * @param other
     *            an instance of the {@link EventDataQueryFieldFilter}
     */
    public EventDataQueryFieldFilter(EventDataQueryFieldFilter other) {
        if (other.document != null) {
            this.document = new Key(other.document);
        }
        this.maxNextCount = other.maxNextCount;
        this.fields = new TreeSet<>(other.fields);
        // need to create a separate parser as the parser is not thread safe
        this.parser = new EventKey();
        // do not copy nextCount or currentField because that is internal state
        this.nextCount = 0;
        this.currentField = null;
    }

    /**
     * Builder-style method used to set the fields to retain
     *
     * @param fields
     *            the fields to retain
     * @return the filter
     */
    public EventDataQueryFieldFilter withFields(Set<String> fields) {
        this.fields = new TreeSet<>(fields);
        return this;
    }

    /**
     * Builder-style method used to set the maximum next count
     *
     * @param maxNextCount
     *            the max next count
     * @return the filter
     */
    public EventDataQueryFieldFilter withMaxNextCount(int maxNextCount) {
        this.maxNextCount = maxNextCount;
        return this;
    }

    @Override
    public void startNewDocument(Key document) {
        this.document = document;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.predicate.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        return true;
    }

    @Override
    public boolean apply(@Nullable Map.Entry<Key,String> entry) {
        if (entry == null) {
            return false;
        }
        return apply(entry.getKey(), true);
    }

    @Override
    public boolean peek(@Nullable Map.Entry<Key,String> entry) {
        if (entry == null) {
            return false;
        }
        // equivalent to apply in the event column case, simple redirect
        return apply(entry.getKey(), false);
    }

    /**
     * The field filter applies if the key's field is in the set of fields to retain
     *
     * @param key
     *            the key
     * @param update
     *            flag that indicates if the {@link #nextCount} should be incremented
     * @return true if the key should be retained
     */
    private boolean apply(Key key, boolean update) {
        parser.parse(key);
        String field = parser.getField();
        field = JexlASTHelper.deconstructIdentifier(field);

        if (fields.contains(field)) {
            nextCount = 0; // reset count
            return true;
        } else if (update) {
            if (currentField != null && currentField.equals(field)) {
                // only increment the count for consecutive misses within the same field
                nextCount++;
            } else {
                // new field means new count
                currentField = field;
                nextCount = 0;
            }
        }

        return false;
    }

    /**
     * Not yet implemented for this filter. Not guaranteed to be called
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return null
     */
    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        if (current == null || maxNextCount == -1 || nextCount < maxNextCount) {
            return null;
        }

        parser.parse(current);
        String higher = fields.higher(parser.getField());

        Text columnQualifier;
        if (higher == null) {
            // generate a rollover range
            Text columnFamily = new Text(current.getColumnFamilyData().toString() + '\u0000');
            Key start = new Key(current.getRow(), columnFamily);
            return new Range(start, false, endKey, endKeyInclusive);
        } else {
            // seek to next available field
            columnQualifier = new Text(higher + '\u0000');
            Key start = new Key(current.getRow(), current.getColumnFamily(), columnQualifier);
            return new Range(start, false, endKey, endKeyInclusive);
        }
    }

    @Override
    public int getMaxNextCount() {
        // while technically implemented, do not return the max next count here. This method is only used
        // by the ChainableEventDataQueryFilter which does NOT guarantee that the filter will exclusively
        // be applied to event keys.
        throw new UnsupportedOperationException("EventDataQueryFieldFilter should not be chained with other filters");
    }

    @Override
    public Key transform(Key toLimit) {
        // not required because the EventDataQueryFieldFilter only operates on event keys
        return null;
    }

    @Override
    public EventDataQueryFilter clone() {
        return new EventDataQueryFieldFilter(this);
    }
}
