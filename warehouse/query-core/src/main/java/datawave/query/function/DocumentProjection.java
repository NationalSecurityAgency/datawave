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

    private final boolean includeGroupingContext;
    private final boolean reducedResponse;
    private final Projection projection;

    /**
     * should track document sizes
     */
    private boolean trackSizes = true;

    @Deprecated
    public DocumentProjection() {
        this(false, false);
    }

    @Deprecated
    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse) {
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
        this.projection = new Projection();
    }

    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse, boolean trackSizes, Set<String> projections,
                    Projection.ProjectionType projectionType) {
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
        this.projection = new Projection(projections, projectionType);
        this.trackSizes = trackSizes;
    }

    /*
     * This contstructor will not include the grouping context and will not reduce the document
     */
    @Deprecated
    public DocumentProjection(Set<String> projections, Projection.ProjectionType projectionType) {
        this(false, false, projections, projectionType);
    }

    @Deprecated
    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse, Set<String> projections, Projection.ProjectionType projectionType) {
        this(includeGroupingContext, reducedResponse, true, projections, projectionType);
    }

    public DocumentProjection(boolean includeGroupingContext, boolean isReducedResponse, boolean isTrackSizes, Projection projection) {
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = isReducedResponse;
        this.trackSizes = isTrackSizes;
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
        Document returnDoc = trim(from.getValue());
        return Maps.immutableEntry(from.getKey(), returnDoc);
    }

    private Document trim(Document d) {
        if (log.isTraceEnabled()) {
            log.trace("Applying projection " + projection + " to " + d);
        }
        Map<String,Attribute<? extends Comparable<?>>> dict = d.getDictionary();
        Document newDoc = new Document();

        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
            String fieldName = entry.getKey();
            Attribute<?> attr = entry.getValue();

            if (projection.apply(fieldName)) {

                // If the projection is configured to exclude, we must fully traverse the subtree
                if (projection.isUseExcludes()) {
                    if (attr instanceof Document) {
                        Document newSubDoc = trim((Document) attr);

                        if (0 < newSubDoc.size()) {
                            newDoc.put(fieldName, newSubDoc.copy(), this.includeGroupingContext);
                        }

                        continue;
                    } else if (attr instanceof Attributes) {
                        Attributes subAttrs = trim((Attributes) attr, fieldName);

                        if (0 < subAttrs.size()) {
                            newDoc.put(fieldName, subAttrs.copy(), this.includeGroupingContext);
                        }

                        continue;
                    }
                }

                // We just want to add this subtree
                newDoc.put(fieldName, (Attribute<?>) attr.copy(), this.includeGroupingContext);

            } else if (!projection.isUseExcludes()) {
                // excludes will completely exclude a subtree, but an includes may
                // initially retain a parent whose children do not match the includes,
                // i.e., a child attribute does not match the includes
                if (attr instanceof Document) {
                    Document newSubDoc = trim((Document) attr);

                    if (0 < newSubDoc.size()) {
                        newDoc.put(fieldName, newSubDoc.copy(), this.includeGroupingContext);
                    }
                } else if (attr instanceof Attributes) {
                    // Since Document instances can be nested under attributes and vice versa
                    // all the way down, we need to pass along the fieldName so that when we
                    // have come up with a nested document it can be evaluated by its own name
                    Attributes subAttrs = trim((Attributes) attr, fieldName);

                    if (0 < subAttrs.size()) {
                        newDoc.put(fieldName, subAttrs.copy(), this.includeGroupingContext);
                    }
                }
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Document after projection: " + newDoc);
        }

        return newDoc;
    }

    private Attributes trim(Attributes attrs, String fieldName) {
        Attributes newAttrs = new Attributes(attrs.isToKeep(), trackSizes);
        for (Attribute<? extends Comparable<?>> attr : attrs.getAttributes()) {
            if (attr instanceof Document) {
                Document newAttr = trim((Document) attr);

                if (0 < newAttr.size()) {
                    newAttrs.add(newAttr);
                }
            } else if (attr instanceof Attributes) {
                Attributes newAttr = trim((Attributes) attr, fieldName);

                if (0 < newAttr.size()) {
                    newAttrs.add(newAttr);
                }
            } else if (projection.apply(fieldName)) {
                // If we're trimming an Attributes and find an Attribute that
                // doesn't nest more Attribute's (Document, Attributes), otherwise,
                // we can retain the "singular" Attribute's (Content, Numeric, etc)
                // if it applies
                newAttrs.add(attr);
            }
        }

        return newAttrs;
    }

}
