package datawave.annotation.projection;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import datawave.annotation.protobuf.v1.Annotation;

/**
 * Produces DataWave field names and formatted values from a prepared {@link Annotation}, using the rules in a {@link CompiledSemanticProfile}. Each result is
 * delivered to the caller as a {@link ProjectedField}, with its base field name, grouping context and event value.
 * <p>
 * The caller supplies an annotation whose content and identities have already been prepared. Projection preserves that annotation and its segment/value order.
 * Fields are delivered in configuration binding order, then annotation traversal and selector match order. The ingest handler or microservice applies the
 * configured DataWave Types and performs persistence and index writes.
 */
public final class AnnotationProjector {
    /**
     * Projects an annotation whose extension entries should be read as stored strings. The method builds an immutable view of the annotation properties and
     * delivers each field to the consumer as soon as it is produced.
     * <p>
     * Use {@link #project(ProjectionEvaluationState, CompiledSemanticProfile, Consumer)} when preparation has decoded declared extension entries or when
     * several consumers need to reuse the same prepared property view. If a budget check or consumer fails, its exception propagates and fields already
     * delivered are not rolled back.
     *
     * @param annotation
     *            the prepared annotation, including identities needed by its configured grouping rules
     * @param profile
     *            the compiled field mappings for the annotation's exact annotationType
     * @param work
     *            checks against the caller's shared budget for preparing and projecting this annotation
     * @param output
     *            the consumer of each formatted field, including repeated equal fields
     * @throws ProjectionException
     *             if the annotation type does not match the profile, a required selection is missing, or selected content cannot satisfy the configured rules
     */
    public void project(Annotation annotation, CompiledSemanticProfile profile, ProjectionWorkGuard work, Consumer<ProjectedField> output) {
        Objects.requireNonNull(output, "output");
        requireType(annotation, profile);
        project(ProjectionEvaluationState.from(annotation, work), profile, output);
    }

    /**
     * Projects the annotation properties retained in a prepared state. Decoded extension values and the original resource budget are reused; extension
     * interpretation is not performed again.
     * <p>
     * Budget and consumer exceptions propagate unchanged. Fields already delivered remain with the consumer, and the budget retains work already charged.
     *
     * @param state
     *            the immutable property view and budget created for this annotation
     * @param profile
     *            the compiled field mappings for the state's exact annotationType
     * @param output
     *            the consumer of each formatted field, including repeated equal fields
     * @throws ProjectionException
     *             if the annotation type does not match the profile, a required selection is missing, or selected content cannot satisfy the configured rules
     */
    public void project(ProjectionEvaluationState state, CompiledSemanticProfile profile, Consumer<ProjectedField> output) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(output, "output");
        requireType(state.annotation, profile);
        for (CompiledSemanticProfile.Binding binding : profile.bindings) {
            switch (binding.scope) {
                case ANNOTATION:
                    projectFrame(state, binding, state.root, null, output);
                    break;
                case SOURCE:
                    state.work.chargeSelectorSteps(1);
                    Object source = state.root.get("source");
                    if (source != null) {
                        projectFrame(state, binding, object(source), null, output);
                    }
                    break;
                case SEGMENT:
                case VALUE:
                    state.work.chargeSelectorSteps(1);
                    for (Object segmentNode : (List<?>) state.root.get("segments")) {
                        Map<String,Object> segment = object(segmentNode);
                        if (binding.scope == CompiledSemanticProfile.Scope.SEGMENT) {
                            projectFrame(state, binding, segment, null, output);
                        } else {
                            // Enter the enclosing segment and look up its values before entering individual value frames.
                            state.work.chargeSelectorSteps(2);
                            for (Object valueNode : (List<?>) segment.get("values")) {
                                Map<String,Object> value = object(valueNode);
                                projectFrame(state, binding, value, segment, output);
                            }
                        }
                    }
                    break;
                default:
                    throw new AssertionError(binding.scope);
            }
        }
    }

    private static void requireType(Annotation annotation, CompiledSemanticProfile profile) {
        Objects.requireNonNull(annotation, "annotation");
        Objects.requireNonNull(profile, "profile");
        if (!profile.annotationType().equals(annotation.getAnnotationType())) {
            throw new ProjectionException("Profile " + profile.id() + " requires annotationType " + profile.annotationType() + ", received "
                            + annotation.getAnnotationType());
        }
    }

    /**
     * Applies one configured binding to the current annotation, source, segment or value object. A required binding must find a value in that object; a present
     * blank string satisfies this requirement even though it produces no field. Value grouping also needs the containing segment's identity.
     */
    private static void projectFrame(ProjectionEvaluationState state, CompiledSemanticProfile.Binding binding, Map<String,Object> frame,
                    Map<String,Object> segment, Consumer<ProjectedField> output) {
        state.work.chargeSelectorSteps(1);
        boolean present = binding.expression.evaluate(frame, state.work, selected -> {
            String text = binding.format.render(selected, state.work, binding.context);
            List<String> group = groupingComponents(state, binding, segment, frame, text);
            // A present blank satisfies required selection but does not produce a candidate. Do not omit again after trim.
            if (!SemanticDefinition.blank(text)) {
                state.work.chargeCandidate();
                output.accept(new ProjectedField(binding.id, binding.field, group, text.trim()));
            }
        });
        if (binding.required && !present) {
            throw new ProjectionException(binding.context + ": required selection missed");
        }
    }

    /**
     * Builds the grouping context without rewriting supplied identities. Value bindings use the source, segment and value hashes. Source-label bindings use the
     * source hash, the configured ALL hash and a hash of the composed label before final trimming.
     */
    private static List<String> groupingComponents(ProjectionEvaluationState state, CompiledSemanticProfile.Binding binding, Map<String,Object> segment,
                    Map<String,Object> value, String text) {
        if (binding.grouping == CompiledSemanticProfile.Grouping.NONE) {
            return List.of();
        }
        if (!state.annotation.hasSource() || SemanticDefinition.blank(state.annotation.getSource().getAnalyticHash())) {
            throw new ProjectionException(binding.context + ": requires an accepted source analyticHash");
        }
        String source = state.annotation.getSource().getAnalyticHash();
        if (binding.grouping == CompiledSemanticProfile.Grouping.SOURCE_LABEL) {
            return List.of(source, binding.allHash, CompiledSemanticProfile.scalarHash(text));
        }
        String segmentHash = (String) segment.get("segmentHash");
        String valueHash = (String) value.get("valueHash");
        if (SemanticDefinition.blank(segmentHash) || SemanticDefinition.blank(valueHash)) {
            throw new ProjectionException(binding.context + ": requires accepted segmentHash and valueHash");
        }
        return List.of(source, segmentHash, valueHash);
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Object> object(Object value) {
        return (Map<String,Object>) value;
    }
}
