package datawave.annotation.projection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;

/**
 * Selects properties from the current annotation, source, segment or value object using the supported {@code scoped-paths-v1} paths. For example,
 * {@code $.score} selects the current value's score, while {@code $.extension.stats.values[*]} visits each element of a prepared extension array.
 * <p>
 * Compilation validates the complete path and checks known protobuf properties before creating Jayway JSONPath lookups. Wildcards are enumerated here so their
 * work is checked against the annotation's budget before collecting or sorting children. Array order is retained; map keys use exact
 * {@link String#compareTo(String)} order. Each library lookup addresses a single property or index.
 */
final class ScopedSelector {
    /** Identifies the selector grammar accepted during profile setup and named in parser diagnostics. */
    static final String DIALECT = "scoped-paths-v1";

    enum Kind {
        // DYNAMIC is a declared extension entry: its decoded JSON kind is validated at runtime.
        STRING, FLOAT, NUMBER, BOOLEAN, OBJECT, ARRAY, DYNAMIC
    }

    private static final Configuration PROVIDER = Configuration.builder().jsonProvider(new JsonSmartJsonProvider()).options(Option.ALWAYS_RETURN_LIST).build();
    private final List<Step> steps;
    private final String context;
    final Kind resultKind;
    final boolean multiple;

    private ScopedSelector(List<Step> steps, Kind resultKind, String context) {
        this.steps = List.copyOf(steps);
        this.context = context;
        this.resultKind = resultKind;
        this.multiple = steps.stream().anyMatch(step -> step.wildcard);
    }

    static ScopedSelector compile(String path, Descriptor frame, String context) {
        try {
            Parser parser = new Parser(path);
            List<Step> steps = parser.parse();
            Shape shape = Shape.object(frame);
            for (Step step : steps) {
                shape = shape.child(step);
            }
            return new ScopedSelector(steps, shape.kind, context);
        } catch (JsonPathException invalid) {
            throw new ProjectionException("Library rejected validated selector " + path, invalid);
        }
    }

    /**
     * Delivers every selected value and returns whether any match exists. Selecting an array itself delivers one array value; an explicit child wildcard is
     * needed to deliver its elements separately. Missing properties and out-of-range indexes return absence, while type, budget and consumer failures
     * propagate.
     */
    boolean select(Object frame, ProjectionWorkGuard work, Consumer<Object> output) {
        return visit(frame, 0, work, output);
    }

    private boolean visit(Object node, int offset, ProjectionWorkGuard work, Consumer<Object> output) {
        if (offset == steps.size()) {
            output.accept(node);
            return true;
        }
        Step step = steps.get(offset);
        work.chargeSelectorSteps(1);
        if (step.wildcard) {
            boolean matched = false;
            if (node instanceof Map) {
                Map<?,?> map = (Map<?,?>) node;
                work.chargeSelectorSteps(map.size());
                List<String> keys = new ArrayList<>();
                for (Object key : map.keySet()) {
                    keys.add((String) key);
                }
                Collections.sort(keys);
                for (String key : keys) {
                    if (visit(map.get(key), offset + 1, work, output)) {
                        matched = true;
                    }
                }
            } else if (node instanceof List) {
                List<?> list = (List<?>) node;
                work.chargeSelectorSteps(list.size());
                for (Object child : list) {
                    if (visit(child, offset + 1, work, output)) {
                        matched = true;
                    }
                }
            } else {
                throw new ProjectionException(context + ": child wildcard requires an object or array");
            }
            return matched;
        }
        if ((step.key != null && !(node instanceof Map)) || (step.key == null && !(node instanceof List))) {
            throw new ProjectionException(context + ": selector step requires " + (step.key == null ? "an array" : "an object"));
        }
        List<?> matches;
        try {
            matches = step.path.read(node, PROVIDER);
        } catch (PathNotFoundException missing) {
            // A missing property or out-of-range index is absence, never a fabricated null.
            return false;
        } catch (JsonPathException invalid) {
            throw new ProjectionException(context + ": typed selector lookup failed", invalid);
        }
        if (!matches.isEmpty()) {
            return visit(matches.get(0), offset + 1, work, output);
        }
        return false;
    }

    /**
     * Holds one validated property, array index or child wildcard. Property and index steps have compiled library lookups; a wildcard is visited directly by
     * this adapter so its work can be checked before enumeration.
     */
    private static final class Step {
        final String key;
        final boolean wildcard;
        final JsonPath path;

        private Step(String key, boolean wildcard, JsonPath path) {
            this.key = key;
            this.wildcard = wildcard;
            this.path = path;
        }

        static Step key(String key) {
            return new Step(key, false, JsonPath.compile("$['" + escape(key) + "']"));
        }

        static Step index(int index) {
            return new Step(null, false, JsonPath.compile("$[" + index + "]"));
        }

        static Step wildcard() {
            return new Step(null, true, null);
        }

        private static String escape(String key) {
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < key.length(); i++) {
                char c = key.charAt(i);
                if (c == '\\' || c == '\'') {
                    text.append('\\').append(c);
                } else if (c < 32) {
                    text.append(String.format("\\u%04x", (int) c));
                } else {
                    text.append(c);
                }
            }
            return text.toString();
        }
    }

    /**
     * Tracks the expected property type while compiling a selector. Protobuf descriptors identify message properties, and map/list element types determine
     * which traversal steps are allowed. An extension entry's decoded type depends on its prepared content, so its final type check happens during evaluation.
     */
    private static final class Shape {
        final Kind kind;
        final Descriptor descriptor;
        final Shape child;

        private Shape(Kind kind, Descriptor descriptor, Shape child) {
            this.kind = kind;
            this.descriptor = descriptor;
            this.child = child;
        }

        static Shape object(Descriptor descriptor) {
            return new Shape(Kind.OBJECT, descriptor, null);
        }

        Shape child(Step step) {
            if (kind == Kind.DYNAMIC) {
                return this;
            }
            if (kind == Kind.ARRAY) {
                if (step.key != null) {
                    throw new ProjectionException("Array selectors require an index or child wildcard");
                }
                return child;
            }
            if (kind != Kind.OBJECT) {
                throw new ProjectionException("Selector traverses a declared scalar");
            }
            if (descriptor == null) {
                if (step.key == null && !step.wildcard) {
                    throw new ProjectionException("Map selectors require a property or child wildcard");
                }
                return child;
            }
            if (step.wildcard) {
                // Every current annotation message has incompatible scalar kinds or declared containers.
                // Select its named map/list first rather than postponing a known scalar-format failure.
                throw new ProjectionException("Wildcard over " + descriptor.getName() + " selects incompatible schema kinds; select a named map or list");
            }
            for (FieldDescriptor field : descriptor.getFields()) {
                if (field.getJsonName().equals(step.key)) {
                    if (field.isMapField()) {
                        Shape mapValue = field.getJsonName().equals(ProjectionEvaluationState.EXTENSION_PROPERTY) ? new Shape(Kind.DYNAMIC, null, null)
                                        : fieldShape(field.getMessageType().findFieldByName("value"));
                        return new Shape(Kind.OBJECT, null, mapValue);
                    }
                    Shape value = fieldShape(field);
                    return field.isRepeated() ? new Shape(Kind.ARRAY, null, value) : value;
                }
            }
            throw new ProjectionException("Unknown property " + step.key + " in " + descriptor.getName());
        }

        private static Shape fieldShape(FieldDescriptor field) {
            switch (field.getJavaType()) {
                case MESSAGE:
                    return object(field.getMessageType());
                case STRING:
                case ENUM:
                    return new Shape(Kind.STRING, null, null);
                case FLOAT:
                    return new Shape(Kind.FLOAT, null, null);
                case INT:
                case LONG:
                    return new Shape(Kind.NUMBER, null, null);
                case BOOLEAN:
                    return new Shape(Kind.BOOLEAN, null, null);
                default:
                    throw new ProjectionException("Unsupported annotation scalar: " + field.getJavaType());
            }
        }
    }

    /** Parses the whole selector independently; permissive library parsing cannot accept an incomplete or unsupported suffix. */
    private static final class Parser {
        final String text;
        int offset;

        Parser(String text) {
            this.text = text;
        }

        List<Step> parse() {
            if (text.isEmpty() || text.charAt(0) != '$') {
                throw invalid();
            }
            offset = 1;
            List<Step> steps = new ArrayList<>();
            while (offset < text.length()) {
                char c = text.charAt(offset++);
                if (c == '.') {
                    if (take('*')) {
                        steps.add(Step.wildcard());
                    } else {
                        int start = offset;
                        if (offset == text.length() || !identifierStart(text.charAt(offset++))) {
                            throw invalid();
                        }
                        while (offset < text.length() && identifierPart(text.charAt(offset))) {
                            offset++;
                        }
                        steps.add(Step.key(text.substring(start, offset)));
                    }
                } else if (c == '[') {
                    if (take('*')) {
                        steps.add(Step.wildcard());
                    } else if (offset < text.length() && (text.charAt(offset) == '\'' || text.charAt(offset) == '"')) {
                        steps.add(Step.key(quoted()));
                    } else {
                        int start = offset;
                        while (offset < text.length() && text.charAt(offset) >= '0' && text.charAt(offset) <= '9') {
                            offset++;
                        }
                        if (offset == start) {
                            throw invalid();
                        }
                        try {
                            steps.add(Step.index(Integer.parseInt(text.substring(start, offset))));
                        } catch (NumberFormatException invalidIndex) {
                            throw invalid();
                        }
                    }
                    if (!take(']')) {
                        throw invalid();
                    }
                } else {
                    throw invalid();
                }
            }
            return steps;
        }

        private String quoted() {
            char quote = text.charAt(offset++);
            StringBuilder key = new StringBuilder();
            while (offset < text.length()) {
                char c = text.charAt(offset++);
                if (c == quote) {
                    String result = key.toString();
                    validateUnicode(result);
                    return result;
                }
                if (c < 32) {
                    throw invalid();
                }
                if (c == '\\') {
                    if (offset == text.length()) {
                        throw invalid();
                    }
                    c = text.charAt(offset++);
                    switch (c) {
                        case '"':
                        case '\\':
                        case '/':
                            key.append(c);
                            break;
                        case '\'':
                            if (quote != '\'') {
                                throw invalid();
                            }
                            key.append(c);
                            break;
                        case 'b':
                            key.append('\b');
                            break;
                        case 'f':
                            key.append('\f');
                            break;
                        case 'n':
                            key.append('\n');
                            break;
                        case 'r':
                            key.append('\r');
                            break;
                        case 't':
                            key.append('\t');
                            break;
                        case 'u':
                            if (offset + 4 > text.length()) {
                                throw invalid();
                            }
                            int code = 0;
                            for (int i = 0; i < 4; i++) {
                                char hex = text.charAt(offset++);
                                int digit = hexDigit(hex);
                                if (digit < 0) {
                                    throw invalid();
                                }
                                code = code * 16 + digit;
                            }
                            key.append((char) code);
                            break;
                        default:
                            throw invalid();
                    }
                } else {
                    key.append(c);
                }
            }
            throw invalid();
        }

        private void validateUnicode(String key) {
            for (int i = 0; i < key.length(); i++) {
                char c = key.charAt(i);
                if (Character.isHighSurrogate(c)) {
                    if (++i == key.length() || !Character.isLowSurrogate(key.charAt(i))) {
                        throw invalid();
                    }
                } else if (Character.isLowSurrogate(c)) {
                    throw invalid();
                }
            }
        }

        private boolean take(char c) {
            if (offset < text.length() && text.charAt(offset) == c) {
                offset++;
                return true;
            }
            return false;
        }

        private ProjectionException invalid() {
            return new ProjectionException("Unsupported " + DIALECT + " selector at offset " + offset + ": " + text);
        }

        private static boolean identifierStart(char c) {
            return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
        }

        private static boolean identifierPart(char c) {
            return identifierStart(c) || (c >= '0' && c <= '9');
        }

        /** JSON Unicode escapes accept ASCII hexadecimal digits, unlike the broader Character.digit predicate. */
        private static int hexDigit(char c) {
            if (c >= '0' && c <= '9') {
                return c - '0';
            }
            if (c >= 'a' && c <= 'f') {
                return c - 'a' + 10;
            }
            if (c >= 'A' && c <= 'F') {
                return c - 'A' + 10;
            }
            return -1;
        }
    }
}
