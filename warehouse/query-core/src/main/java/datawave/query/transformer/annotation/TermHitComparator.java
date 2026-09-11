package datawave.query.transformer.annotation;

import java.util.Comparator;

import datawave.query.transformer.annotation.model.TermHit;

/**
 * Compares TermHits by confidence, sorting in descending order
 */
public class TermHitComparator implements Comparator<TermHit> {
    @Override
    public int compare(TermHit o1, TermHit o2) {
        return Float.compare(o1.getConfidence(), o2.getConfidence());
    }
}
