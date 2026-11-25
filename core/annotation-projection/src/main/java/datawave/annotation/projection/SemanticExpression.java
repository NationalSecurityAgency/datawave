package datawave.annotation.projection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.protobuf.Descriptors.Descriptor;

/**
 * Represents a compiled rule for selecting, formatting or combining annotation properties. Evaluation delivers selected values to its consumer and separately
 * reports whether a value was present.
 * <p>
 * A selector may produce several matches. Formatting and composition operands must be known during compilation to produce at most one result; wildcard results
 * cannot be paired by position to combine unrelated annotation values.
 */
abstract class SemanticExpression {
    private static final int MAX_EXPRESSION_DEPTH = 32;
    private static final String JOIN_NONBLANK = "join-nonblank";
    final ScopedSelector.Kind kind;
    final boolean multiple;

    SemanticExpression(ScopedSelector.Kind kind, boolean multiple) {
        this.kind = kind;
        this.multiple = multiple;
    }

    /**
     * Delivers each present result from the current scope object. An absent result does not invoke the consumer. A present blank string still counts as a
     * result, even when the projector later omits it from field output. Budget, formatting and consumer failures propagate.
     *
     * @return true if at least one result was present, including a blank string; false if the expression produced no result
     */
    abstract boolean evaluate(Object frame, ProjectionWorkGuard work, Consumer<Object> output);

    /**
     * Evaluates an operand that compilation has restricted to at most one result. Null denotes absence because actual null values are rejected when the
     * property view is built. Composition can therefore use the selected value directly without allocating a collection of matches.
     */
    private Object singleValue(Object frame, ProjectionWorkGuard work) {
        Object[] selected = new Object[1];
        evaluate(frame, work, value -> selected[0] = value);
        return selected[0];
    }

    static SemanticExpression compile(Object definition, Descriptor scope, int depth, String context) {
        if (depth > MAX_EXPRESSION_DEPTH) {
            throw new ProjectionException("Semantic expression nesting exceeds " + MAX_EXPRESSION_DEPTH + " levels");
        }
        Map<?,?> object = SemanticDefinition.object(definition);
        String operation = SemanticDefinition.string(object.get("op"));
        switch (operation) {
            case "select":
                SemanticDefinition.keys(object, "op", "path");
                ScopedSelector selector = ScopedSelector.compile(SemanticDefinition.string(object.get("path")), scope, context);
                return new SemanticExpression(selector.resultKind, selector.multiple) {
                    @Override
                    boolean evaluate(Object frame, ProjectionWorkGuard work, Consumer<Object> output) {
                        return selector.select(frame, work, output);
                    }
                };
            case "literal":
                SemanticDefinition.keys(object, "op", "value");
                String literal = SemanticDefinition.string(object.get("value"));
                return new SemanticExpression(ScopedSelector.Kind.STRING, false) {
                    @Override
                    boolean evaluate(Object frame, ProjectionWorkGuard work, Consumer<Object> output) {
                        work.checkStringLength(literal.length());
                        output.accept(literal);
                        return true;
                    }
                };
            case "format":
                SemanticDefinition.keys(object, "op", "format", "operand");
                ScalarFormat format = ScalarFormat.named(SemanticDefinition.string(object.get("format")));
                SemanticExpression operand = compile(object.get("operand"), scope, depth + 1, context);
                definite(operand);
                format.requireCompatible(operand.kind);
                return new SemanticExpression(ScopedSelector.Kind.STRING, false) {
                    @Override
                    boolean evaluate(Object frame, ProjectionWorkGuard work, Consumer<Object> output) {
                        return operand.evaluate(frame, work, value -> output.accept(format.render(value, work, context)));
                    }
                };
            case "concat":
            case JOIN_NONBLANK:
                SemanticDefinition.keys(object, "op", "delimiter", "operands");
                String delimiter = SemanticDefinition.string(object.get("delimiter"));
                List<SemanticExpression> operands = new ArrayList<>();
                for (Object child : SemanticDefinition.nonemptyList(object.get("operands"))) {
                    SemanticExpression expression = compile(child, scope, depth + 1, context);
                    definite(expression);
                    ScalarFormat.TEXT.requireCompatible(expression.kind);
                    operands.add(expression);
                }
                return new Composition(delimiter, List.copyOf(operands), operation.equals(JOIN_NONBLANK), context);
            default:
                throw new ProjectionException("Unknown semantic operation: " + operation);
        }
    }

    private static void definite(SemanticExpression expression) {
        if (expression.multiple) {
            throw new ProjectionException("Composition and format operands must be statically single-result expressions");
        }
    }

    private static final class Composition extends SemanticExpression {
        final String delimiter;
        final List<SemanticExpression> operands;
        final boolean skipBlank;
        final String context;

        Composition(String delimiter, List<SemanticExpression> operands, boolean skipBlank, String context) {
            super(ScopedSelector.Kind.STRING, false);
            this.delimiter = delimiter;
            this.operands = operands;
            this.skipBlank = skipBlank;
            this.context = context;
        }

        @Override
        boolean evaluate(Object frame, ProjectionWorkGuard work, Consumer<Object> output) {
            StringBuilder composed = new StringBuilder();
            boolean present = false;
            for (SemanticExpression operand : operands) {
                Object selected = operand.singleValue(frame, work);
                if (selected == null) {
                    if (skipBlank) {
                        continue;
                    }
                    return false;
                }
                String text = ScalarFormat.TEXT.render(selected, work, context);
                if (skipBlank && SemanticDefinition.blank(text)) {
                    continue;
                }
                long length = (long) composed.length() + text.length() + (present ? delimiter.length() : 0);
                work.checkStringLength(length);
                if (present) {
                    composed.append(delimiter);
                }
                composed.append(text);
                present = true;
            }
            if (present) {
                output.accept(composed.toString());
            }
            return present;
        }
    }
}
