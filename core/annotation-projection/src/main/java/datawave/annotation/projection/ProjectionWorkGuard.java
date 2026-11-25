package datawave.annotation.projection;

/**
 * Lets the caller enforce resource limits while preparing annotation properties and projecting fields. The caller supplies one implementation backed by the
 * annotation's shared budget; {@link ProjectionEvaluationState} and {@link AnnotationProjector} invoke it before the work described by each method.
 * <p>
 * Each method must throw if the requested work would exceed its limit. Budget exceptions propagate unchanged. Projection does not reset counters or refund work
 * when the consumer removes duplicate fields or discards output.
 */
public interface ProjectionWorkGuard {
    /**
     * Counts property traversal before entering an annotation, source, segment or value object, attempting a lookup, or enumerating wildcard children. Missing
     * properties and out-of-range array indexes still consume lookup work.
     *
     * @param additionalSteps
     *            the number of traversal steps to add to the annotation's cumulative work
     */
    void chargeSelectorSteps(long additionalSteps);

    /**
     * Counts one field after formatting and blank omission, before constructing its {@link ProjectedField} and invoking the consumer. Repeated equal fields
     * each count once, even when the consumer later removes duplicates or chooses not to store them.
     */
    void chargeCandidate();

    /**
     * Checks a temporary string's proposed size before appending composed text or rendering a number. The size is measured in UTF-16 code units, as returned by
     * {@link String#length()}. The caller separately enforces its UTF-8 payload and field-length policies, including any WARN or DROP behavior.
     *
     * @param prospectiveLength
     *            the proposed size or a conservative upper bound for the temporary string
     */
    void checkStringLength(long prospectiveLength);

    /**
     * Counts one object, array or scalar in the annotation property view before allocating its representation. The implementation checks both the cumulative
     * node count and the container nesting limit.
     *
     * @param containerDepth
     *            the container depth, with the root annotation object at one; a scalar uses the depth of its containing object or array
     */
    void chargeTreeNode(int containerDepth);
}
