package datawave.query.predicate;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class EventDataQueryFieldFilter implements EventDataQueryFilter {
    private Set<String> nonEventFields;

    private final KeyProjection projection;

    public EventDataQueryFieldFilter(EventDataQueryFieldFilter other) {
        this.nonEventFields = other.nonEventFields;
        if (other.document != null) {
            document = new Key(other.document);
        }
        this.projection = other.getProjection();
    }

    /**
     * Initialize filter with an empty projection
     */
    public EventDataQueryFieldFilter() {
        this.projection = new KeyProjection();
    }

    /**
     * Initiate from a KeyProjection
     *
     * @param projection
     */
    public EventDataQueryFieldFilter(KeyProjection projection) {
        this.projection = projection;
    }

    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     *
     * @param script
     *            a script
     * @param nonEventFields
     *            a set of non-event fields
     */
    @Deprecated
    public EventDataQueryFieldFilter(ASTJexlScript script, Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;

        Set<String> queryFields = Sets.newHashSet();
        for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(script)) {
            queryFields.add(JexlASTHelper.deconstructIdentifier(identifier));
        }

        this.projection = new KeyProjection();
        this.projection.setIncludes(queryFields);
    }

    protected Key document = null;

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

    public KeyProjection getProjection() {
        return projection;
    }

    @Override
    public boolean apply(@Nullable Map.Entry<Key,String> input) {
        return projection.apply(input);
    }

    @Override
    public boolean peek(@Nullable Map.Entry<Key,String> input) {
        return projection.peek(input);
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
        // not yet implemented
        return null;
    }

    @Override
    public int getMaxNextCount() {
        // not yet implemented
        return -1;
    }

    @Override
    public Key transform(Key toLimit) {
        // not yet implemented
        return null;
    }

    @Override
    public EventDataQueryFilter clone() {
        return new EventDataQueryFieldFilter(this);
    }

    /**
     * Configure the delegate {@link Projection} with the fields to exclude
     *
     * @param excludes
     *            the set of fields to exclude
     * @deprecated This method is deprecated and should no longer be used.
     */
    @Deprecated
    public void setExcludes(Set<String> excludes) {
        this.projection.setExcludes(excludes);
    }

    /**
     * Set the delegate {@link Projection} with the fields to include
     *
     * @param includedFields
     *            the sorted set of fields to include
     * @deprecated This method is deprecated and should no longer be used.
     */
    @Deprecated
    public void setIncludes(Set<String> includedFields) {
        this.projection.setIncludes(includedFields);
    }

}
