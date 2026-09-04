package datawave.query.transformer;

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
 * field matched, so dropping it merges hit terms that differ only by their context. Added last so that every other transform still sees the context.
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

        // the common case is that nothing needs rewriting, so do not build a replacement until we know otherwise
        if (!requiresRewrite(hitTerms)) {
            return entry;
        }

        Attributes rewritten = new Attributes(hitTerms.isToKeep());
        if (hitTerms instanceof Attributes) {
            for (Attribute<? extends Comparable<?>> attribute : ((Attributes) hitTerms).getRawAttributes()) {
                rewritten.add(rewriteHitTerm(attribute));
            }
        } else {
            rewritten.add(rewriteHitTerm(hitTerms));
        }

        document.remove(HIT_TERM_FIELD);
        document.put(HIT_TERM_FIELD, rewritten);

        return entry;
    }

    private boolean requiresRewrite(Attribute<? extends Comparable<?>> hitTerms) {
        if (hitTerms instanceof Attributes) {
            for (Attribute<? extends Comparable<?>> attribute : ((Attributes) hitTerms).getRawAttributes()) {
                if (baseField(attribute) != null) {
                    return true;
                }
            }
            return false;
        }
        return baseField(hitTerms) != null;
    }

    private Attribute<? extends Comparable<?>> rewriteHitTerm(Attribute<? extends Comparable<?>> attribute) {
        String baseField = baseField(attribute);
        if (baseField == null) {
            return attribute;
        }

        Content hitTerm = (Content) attribute;
        String term = hitTerm.getContent();
        Content rewritten = new Content(baseField + term.substring(term.indexOf(':')), hitTerm.getMetadata(), hitTerm.isToKeep(), hitTerm.getSource());
        rewritten.setColumnVisibility(hitTerm.getColumnVisibility());
        return rewritten;
    }

    /**
     * @return the field name without its grouping context, or null if this hit term does not carry one
     */
    private String baseField(Attribute<? extends Comparable<?>> attribute) {
        if (!(attribute instanceof Content)) {
            return null;
        }

        // only the field name half of the hit term is examined, a value is free to contain dots of its own
        String term = ((Content) attribute).getContent();
        int split = term.indexOf(':');
        if (split == -1) {
            return null;
        }

        String field = term.substring(0, split);
        return JexlASTHelper.hasGroupingContext(field) ? JexlASTHelper.removeGroupingContext(field) : null;
    }
}
