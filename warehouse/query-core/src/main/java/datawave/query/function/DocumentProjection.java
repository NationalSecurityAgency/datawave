package datawave.query.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.predicate.Projection;

/**
 * Applies an includes or excludes projection to a Document. Includes projection will preserve Document sub-substructure whereas excludes projection will prune
 * sub-substructure which does not match the excludes.
 * <p>
 * e.g. Input: {NAME:'bob', CHILDREN:[{NAME:'frank', AGE:12}, {NAME:'sally', AGE:10}], AGE:40}
 * <p>
 * Include of 'NAME' applied: {NAME:'bob', CHILDREN:[{NAME:'frank'}, {NAME:'sally'}]}
 * <p>
 * Exclude of 'NAME' applied: {CHILDREN:[{AGE:12}, {AGE:10}], AGE:40}
 */
public class DocumentProjection implements DocumentPermutation {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DocumentProjection.class);

    private final Projection projection;

    public DocumentProjection(Set<String> projections, Projection.ProjectionType projectionType) {
        this(new Projection(projections, projectionType));
    }

    public DocumentProjection(Projection projection) {
        this.projection = projection;
    }

    @Deprecated
    public void setIncludes(Set<String> includes) {
        this.projection.setIncludes(includes);
    }

    @Deprecated
    public void setExcludes(Set<String> excludes) {
        this.projection.setExcludes(excludes);
    }

    public Projection getProjection() {
        return projection;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> from) {
        trim(from.getValue());
        return Maps.immutableEntry(from.getKey(), from.getValue());
    }

    private void trim(Document d) {
        if (log.isTraceEnabled()) {
            log.trace("Applying projection " + projection + " to " + d);
        }
        Map<String,Attribute<? extends Comparable<?>>> dict = d.getDictionary();

        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
            String fieldName = entry.getKey();
            Attribute<?> attr = entry.getValue();
            attr.setToKeep(attr.isToKeep() && projection.apply(fieldName));
        }

        // reduce the document to those to keep
        d.reduceToKeep();
    }

}
