package datawave.query.transformer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.JexlASTHelper;

/**
 * Removes the grouping context from the values of the {@code HIT_TERM} field, rewriting a hit term like {@code NAME.FOO.1:bob} to {@code NAME:bob}.
 * <p>
 * Opt in through the {@link datawave.query.QueryParameters#STRIP_HIT_TERM_GROUPING_CONTEXT} query parameter: the context says which group of a multi-valued
 * field matched, so dropping it merges hit terms that differ only by their context. Hit terms are only merged when their metadata, including visibility and
 * timestamp, also match. Added last so that every other transform still sees the context.
 */
public class RemoveHitTermGroupingContextTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final String HIT_TERM_FIELD = JexlEvaluation.HIT_TERM_FIELD;

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry == null || entry.getValue() == null) {
            return entry;
        }

        Document document = entry.getValue();
        Attribute<? extends Comparable<?>> hitTerms = document.getDictionary().get(HIT_TERM_FIELD);
        if (hitTerms == null) {
            return entry;
        }

        Attributes rewritten = hitTerms instanceof Attributes ? rewriteHitTerms((Attributes) hitTerms) : rewriteHitTerm(hitTerms);
        if (rewritten != null) {
            document.remove(HIT_TERM_FIELD);
            document.put(HIT_TERM_FIELD, rewritten);
        }

        return entry;
    }

    /**
     * Rewrites each hit term in a single pass. The common case is that nothing needs rewriting, so the replacement is not built until the first hit term that
     * carries a grouping context is found.
     *
     * @return the rewritten hit terms, or null if none carried a grouping context
     */
    private Attributes rewriteHitTerms(Attributes hitTerms) {
        Collection<Attribute<? extends Comparable<?>>> attributes = hitTerms.getRawAttributes();
        Attributes rewritten = null;
        int unchanged = 0;

        for (Attribute<? extends Comparable<?>> attribute : attributes) {
            Attribute<? extends Comparable<?>> hitTerm = rewrite(attribute);
            if (rewritten == null) {
                if (hitTerm == attribute) {
                    unchanged++;
                    continue;
                }

                // first rewrite, carry over the hit terms already passed
                rewritten = new Attributes(hitTerms.isToKeep());
                Iterator<Attribute<? extends Comparable<?>>> passed = attributes.iterator();
                for (int i = 0; i < unchanged; i++) {
                    rewritten.add(passed.next());
                }
            }
            rewritten.add(hitTerm);
        }

        return rewritten;
    }

    /**
     * @return the rewritten hit term wrapped in an {@link Attributes}, or null if it did not carry a grouping context
     */
    private Attributes rewriteHitTerm(Attribute<? extends Comparable<?>> attribute) {
        Attribute<? extends Comparable<?>> hitTerm = rewrite(attribute);
        if (hitTerm == attribute) {
            return null;
        }

        Attributes rewritten = new Attributes(attribute.isToKeep());
        rewritten.add(hitTerm);
        return rewritten;
    }

    /**
     * @return the hit term without its grouping context, or the same instance if it does not carry one
     */
    private Attribute<? extends Comparable<?>> rewrite(Attribute<? extends Comparable<?>> attribute) {
        if (!(attribute instanceof Content)) {
            return attribute;
        }

        Content hitTerm = (Content) attribute;
        String term = hitTerm.getContent();
        int split = term.indexOf(':');
        if (split == -1) {
            return attribute;
        }

        // only the field name half of the hit term is examined, a value is free to contain dots of its own
        int context = term.indexOf(JexlASTHelper.GROUPING_CHARACTER_SEPARATOR);
        if (context == -1 || context > split) {
            return attribute;
        }

        String stripped = new StringBuilder(term.length() - (split - context)).append(term, 0, context).append(term, split, term.length()).toString();
        Content rewritten = new Content(stripped, hitTerm.getMetadata(), hitTerm.isToKeep(), hitTerm.getSource());
        rewritten.setColumnVisibility(hitTerm.getColumnVisibility());
        return rewritten;
    }
}
