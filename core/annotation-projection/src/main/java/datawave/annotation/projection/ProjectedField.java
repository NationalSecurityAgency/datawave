package datawave.annotation.projection;

import java.util.List;
import java.util.Objects;

/**
 * A field produced by {@link AnnotationProjector}, containing its configured base name, grouping context and formatted event value. The caller uses these
 * components to construct the grouped field name, apply its DataWave Type and select event or index destinations.
 * <p>
 * For example, a base of {@code BINARY_CATEGORY}, grouping components {@code [aSource, bAll, vLabel]} and an event value of {@code present} describe the
 * grouped field {@code BINARY_CATEGORY.aSource.bAll.vLabel} with value {@code present}.
 * <p>
 * Each emitted result is retained, including repeated equal fields and different values in the same group. Equality includes the binding ID so results from
 * different configured bindings remain distinguishable. The caller applies any destination-specific duplicate policy after projection has counted each emitted
 * result against the annotation's budget.
 */
public final class ProjectedField {
    private final String bindingId;
    private final String baseFieldName;
    private final List<String> groupingComponents;
    private final String eventValue;

    ProjectedField(String bindingId, String baseFieldName, List<String> groupingComponents, String eventValue) {
        this.bindingId = Objects.requireNonNull(bindingId);
        this.baseFieldName = Objects.requireNonNull(baseFieldName);
        this.groupingComponents = List.copyOf(groupingComponents);
        this.eventValue = Objects.requireNonNull(eventValue);
    }

    /**
     * Identifies the configuration binding that produced this field.
     *
     * @return the binding's configured ID
     */
    public String bindingId() {
        return bindingId;
    }

    /**
     * Returns the field name to which the caller applies DataWave field policies.
     *
     * @return the configured base name, such as {@code BINARY_CATEGORY}, without a grouping suffix
     */
    public String baseFieldName() {
        return baseFieldName;
    }

    /**
     * Returns the ordered components of the field's grouping context. Value grouping uses source, segment and value identities, in that order. Source-label
     * grouping uses the source identity and the derived ALL and label hashes.
     *
     * @return an immutable list of grouping components, or an empty list for an ungrouped field
     */
    public List<String> groupingComponents() {
        return groupingComponents;
    }

    /**
     * Returns the selected or composed field value after formatting and final {@link String#trim()}.
     *
     * @return event text before DataWave Type normalization; an empty string is possible when trim removes non-whitespace control characters
     */
    public String eventValue() {
        return eventValue;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ProjectedField)) {
            return false;
        }
        ProjectedField field = (ProjectedField) other;
        return bindingId.equals(field.bindingId) && baseFieldName.equals(field.baseFieldName) && groupingComponents.equals(field.groupingComponents)
                        && eventValue.equals(field.eventValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bindingId, baseFieldName, groupingComponents, eventValue);
    }

    @Override
    public String toString() {
        return baseFieldName + groupingComponents + "=" + eventValue;
    }
}
