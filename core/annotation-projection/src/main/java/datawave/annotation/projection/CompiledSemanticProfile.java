package datawave.annotation.projection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.hash.Hashing;
import com.google.protobuf.Descriptors.Descriptor;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

/**
 * Validated field mappings for one annotation type. Call {@link #compile(String, String, Map)} during configuration setup, then reuse the result when
 * {@link AnnotationProjector} processes annotations of that exact type.
 * <p>
 * Each binding identifies a base field name, the annotation properties to read, a text format and a grouping rule. Compilation checks these rules and copies
 * them into immutable definitions. The enclosing profile's DataWave Type, Analyzer and storage policies are validated separately by the caller.
 */
public final class CompiledSemanticProfile {
    private static final Pattern BASE = Pattern.compile("[A-Z][A-Z0-9_]*");
    private static final String HASH_STRATEGY = "murmur3-32-unencoded-chars-v1";
    private static final String TRIM_STRATEGY = "java-string-trim";
    // Source-label grouping uses this synthetic segment label rather than an annotation segment's supplied identity.
    private static final String SYNTHETIC_ALL_LABEL = "ALL";
    private final String id;
    private final String annotationType;
    final List<Binding> bindings;

    private CompiledSemanticProfile(String id, String annotationType, List<Binding> bindings) {
        this.id = id;
        this.annotationType = annotationType;
        this.bindings = List.copyOf(bindings);
    }

    /**
     * Compiles the {@code semantic} section of a parsed version-one profile. This section defines how annotation properties become field names and formatted
     * values. The test resource {@code configuration/baseline-profiles.json} contains complete examples for text extraction and content labels.
     * <p>
     * The caller parses the enclosing JSON document, rejects duplicate JSON keys and profile registrations, and validates protected fields and DataWave
     * execution policies. This method checks the semantic rules, including selector paths, formats, grouping and unique binding IDs. It does not load Type or
     * Analyzer classes, and subsequent changes to the supplied map do not change the compiled profile.
     *
     * @param id
     *            the nonblank profile identifier to include in diagnostics, retained exactly as supplied
     * @param annotationType
     *            the nonblank annotationType to accept, matched without aliases or case conversion
     * @param semantic
     *            the parsed semantic object containing selectorDialect, output and bindings
     * @return immutable field mappings that can be reused for annotations of the configured type
     * @throws ProjectionException
     *             if an identifier or semantic rule is invalid or unsupported
     */
    public static CompiledSemanticProfile compile(String id, String annotationType, Map<String,?> semantic) {
        SemanticDefinition.nonblank(id);
        SemanticDefinition.nonblank(annotationType);
        try {
            SemanticDefinition.object(semantic);
            SemanticDefinition.keys(semantic, "selectorDialect", "output", "bindings");
            if (!ScopedSelector.DIALECT.equals(SemanticDefinition.string(semantic.get("selectorDialect")))) {
                throw new ProjectionException("Unsupported selector dialect");
            }
            Map<?,?> output = SemanticDefinition.object(semantic.get("output"));
            SemanticDefinition.keys(output, "trim", "omitBlank");
            if (!TRIM_STRATEGY.equals(SemanticDefinition.string(output.get("trim"))) || !SemanticDefinition.bool(output.get("omitBlank"))) {
                throw new ProjectionException("Unsupported semantic output settings");
            }
            List<Binding> bindings = new ArrayList<>();
            Set<String> ids = new HashSet<>();
            for (Object entry : SemanticDefinition.nonemptyList(semantic.get("bindings"))) {
                Map<?,?> binding = SemanticDefinition.object(entry);
                String bindingId = SemanticDefinition.nonblank(binding.get("id"));
                if (!ids.add(bindingId)) {
                    throw new ProjectionException("Duplicate binding id: " + bindingId);
                }
                try {
                    bindings.add(Binding.compile(binding, bindingId, id));
                } catch (ProjectionException invalid) {
                    throw new ProjectionException("Binding " + bindingId + ": " + invalid.getMessage(), invalid);
                }
            }
            return new CompiledSemanticProfile(id, annotationType, bindings);
        } catch (ProjectionException invalid) {
            throw new ProjectionException("Profile " + id + " (" + annotationType + "): " + invalid.getMessage(), invalid);
        }
    }

    /**
     * Returns the configured profile identifier used in diagnostics.
     *
     * @return the identifier exactly as supplied to {@link #compile(String, String, Map)}
     */
    public String id() {
        return id;
    }

    /**
     * Returns the annotationType this profile accepts. This identifies annotation content; it is separate from a DataWave Type used to normalize field values.
     *
     * @return the exact annotationType string, without aliases or case conversion
     */
    public String annotationType() {
        return annotationType;
    }

    enum Scope {
        ANNOTATION("annotation", Annotation.getDescriptor()),
        SOURCE("source", AnnotationSource.getDescriptor()),
        SEGMENT("segment", Segment.getDescriptor()),
        VALUE("value", SegmentValue.getDescriptor());

        final String name;
        final Descriptor descriptor;

        Scope(String name, Descriptor descriptor) {
            this.name = name;
            this.descriptor = descriptor;
        }

        static Scope named(String name) {
            for (Scope scope : values()) {
                if (scope.name.equals(name)) {
                    return scope;
                }
            }
            throw new ProjectionException("Unknown binding scope: " + name);
        }
    }

    enum Grouping {
        NONE, VALUE, SOURCE_LABEL
    }

    /**
     * Holds one binding's base field name, property expression, output format and grouping rule. Its profile/binding/scope description is prepared once so
     * failures can identify the responsible configuration.
     */
    static final class Binding {
        final String id;
        final String field;
        final Scope scope;
        final boolean required;
        final SemanticExpression expression;
        final ScalarFormat format;
        final Grouping grouping;
        final String allHash;
        final String context;

        private Binding(String id, String field, Scope scope, boolean required, SemanticExpression expression, ScalarFormat format, Grouping grouping,
                        String context) {
            this.id = id;
            this.field = field;
            this.scope = scope;
            this.required = required;
            this.expression = expression;
            this.format = format;
            this.grouping = grouping;
            this.allHash = grouping == Grouping.SOURCE_LABEL ? scalarHash(SYNTHETIC_ALL_LABEL) : null;
            this.context = context;
        }

        static Binding compile(Map<?,?> binding, String id, String profileId) {
            SemanticDefinition.keys(binding, "id", "scope", "field", "required", "expression", "format", "grouping");
            String field = SemanticDefinition.string(binding.get("field"));
            if (!BASE.matcher(field).matches()) {
                throw new ProjectionException("Invalid configured base field: " + field);
            }
            Scope scope = Scope.named(SemanticDefinition.string(binding.get("scope")));
            String context = "Profile " + profileId + ", binding " + id + ", scope " + scope.name;
            boolean required = SemanticDefinition.bool(binding.get("required"));
            SemanticExpression expression = SemanticExpression.compile(binding.get("expression"), scope.descriptor, 1, context);
            ScalarFormat format = ScalarFormat.named(SemanticDefinition.string(binding.get("format")));
            format.requireCompatible(expression.kind);
            Map<?,?> group = SemanticDefinition.object(binding.get("grouping"));
            Grouping grouping;
            switch (SemanticDefinition.string(group.get("kind"))) {
                case "none":
                    SemanticDefinition.keys(group, "kind");
                    grouping = Grouping.NONE;
                    break;
                case "source-segment-value":
                    SemanticDefinition.keys(group, "kind");
                    if (scope != Scope.VALUE) {
                        throw new ProjectionException("Value grouping requires value scope");
                    }
                    grouping = Grouping.VALUE;
                    break;
                case "source-all-label":
                    SemanticDefinition.keys(group, "kind", "hashStrategy");
                    if (scope != Scope.SOURCE || format != ScalarFormat.TEXT || !HASH_STRATEGY.equals(group.get("hashStrategy"))) {
                        throw new ProjectionException("Source-label grouping requires source scope, text and " + HASH_STRATEGY);
                    }
                    grouping = Grouping.SOURCE_LABEL;
                    break;
                default:
                    throw new ProjectionException("Unknown grouping kind: " + group.get("kind"));
            }
            return new Binding(id, field, scope, required, expression, format, grouping, context);
        }
    }

    /**
     * Calculates the configured synthetic hash for ALL and composed source labels. It hashes the original Java characters and returns uppercase hexadecimal
     * text. Supplied annotation, source, segment and value identities are preserved rather than recalculated here.
     */
    static String scalarHash(String text) {
        return Hashing.murmur3_32_fixed().newHasher().putUnencodedChars(text).hash().toString().toUpperCase(java.util.Locale.ROOT);
    }
}
