package datawave.query.analysis;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;

/** Enriches the same AST already accepted by QueryShape; never executes functions or accesses metadata. */
final class QueryFingerprintBuilder {
    private final Set<String> bindings = new TreeSet<>();
    private final Map<String,Integer> counts = new TreeMap<>();
    private final Set<String> topology = new TreeSet<>();
    private final Map<String,Double> measurements = new TreeMap<>();
    private final Set<String> protections = new TreeSet<>();
    private final List<String> diagnostics = new ArrayList<>();
    private final Set<String> fields = new TreeSet<>();
    private int fieldUses;

    QueryFingerprint build(ASTJexlScript script) {
        for (String name : List.of("predicates", "fields", "repeatedFieldUses", "booleanDepth", "junctionWidth", "functionArguments", "regexPrefixLength",
                        "regexSuffixLength", "controlMagnitude")) {
            measurements.put(name, 0.0);
        }
        Parts result = visit(script.jjtGetChild(0), "ROOT", 0);
        measurements.put("fields", (double) fields.size());
        measurements.put("repeatedFieldUses", (double) (fieldUses - fields.size()));
        String profile = result.profile;
        if (!diagnostics.isEmpty()) {
            // Unknown regex/control semantics must not admit a different query merely because its syntax looks similar.
            profile = QueryFingerprint.encode("UNCERTAIN", List.of(profile, QueryFingerprint.digest(JexlStringBuildingVisitor.buildQuery(script))));
        }
        return new QueryFingerprint(result.exact, profile, bindings, counts, topology, measurements, protections, diagnostics);
    }

    private Parts visit(JexlNode original, String context, int depth) {
        JexlNode node = unwrap(original);
        if (node instanceof ASTAndNode || node instanceof ASTOrNode) {
            QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
            if (marker.isAnyType()) {
                if (marker.isType(QueryPropertyMarker.MarkerType.BOUNDED_RANGE)) {
                    try {
                        JexlASTHelper.findRange().getRange(node); // Checks same-field bounds without an indexedOnly metadata helper.
                    } catch (DatawaveFatalQueryException | IllegalArgumentException | ClassCastException e) {
                        // RangeFinder casts both numeric bounds to one type; mixed types must not abort the batch.
                        unknown("Unable to classify bounded range: " + e.getClass().getSimpleName());
                    }
                }
                String label = "MARKER:" + marker.getType().getLabel();
                record(context, label);
                return combine(label, List.of(visit(marker.getSource(), path(context, label), depth)), false);
            }
            String label = node instanceof ASTAndNode ? "AND" : "OR";
            List<JexlNode> flattened = new ArrayList<>();
            flatten(node, node.getClass(), flattened);
            maximum("junctionWidth", flattened.size());
            maximum("booleanDepth", depth + 1);
            record(context, label);
            List<Parts> children = new ArrayList<>();
            for (JexlNode child : flattened) {
                children.add(visit(child, path(context, label), depth + 1));
            }
            return combine(label, children, true);
        }
        if (node instanceof ASTFunctionNode) {
            return function((ASTFunctionNode) node, context, depth);
        }
        String field = fieldName(node);
        if (field != null) {
            fields.add(field);
            fieldUses++;
            bindings.add(QueryFingerprint.encode("binding", List.of(context, field)));
            String role = "_ANYFIELD_".equals(field) || "_NOFIELD_".equals(field) ? "FIELD:" + field : "FIELD";
            return new Parts("FIELD:" + field, role);
        }
        if (node instanceof ASTStringLiteral || node instanceof ASTNumberLiteral || isSignedNumber(node)) {
            return Parts.literal("VALUE");
        }
        if (node instanceof ASTNullLiteral) {
            return Parts.literal("NULL");
        }
        if (node instanceof ASTTrueNode || node instanceof ASTFalseNode) {
            return Parts.literal(node instanceof ASTTrueNode ? "TRUE" : "FALSE");
        }
        String label = node.getClass().getSimpleName();
        boolean negation = node instanceof ASTNotNode;
        if (negation) {
            maximum("booleanDepth", depth + 1);
        } else {
            add("predicates", 1);
        }
        Parts regex = null;
        if (node instanceof ASTERNode || node instanceof ASTNRNode) {
            regex = regex(node.jjtGetChild(1));
            label += ":" + regex.profile;
        }
        record(context, label);
        List<Parts> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            children.add(regex != null && i == 1 ? regex : visit(node.jjtGetChild(i), path(context, label + ":" + i), depth + (negation ? 1 : 0)));
        }
        return combine(label, children, false);
    }

    private Parts function(ASTFunctionNode node, String context, int depth) {
        FunctionJexlNodeVisitor function = FunctionJexlNodeVisitor.eval(node);
        String name = function.namespace() + ":" + function.name();
        List<JexlNode> args = function.args();
        String label = "FUNCTION:" + name + ":" + args.size();
        record(context, label);
        add("predicates", 1);
        add("functionArguments", args.size());
        Map<Integer,Parts> overrides = new HashMap<>();

        if (Set.of("filter:includeRegex", "filter:excludeRegex", "filter:getAllMatches").contains(name)) {
            if (args.size() == 2) {
                overrides.put(1, regex(args.get(1)));
            } else {
                unknown("Expected two arguments for " + name);
            }
        } else if ("filter:matchesAtLeastCountOf".equals(name)) {
            if (args.size() >= 3) {
                overrides.put(0, numericControl(args.get(0), name + ":minimum", context));
                for (int i = 2; i < args.size(); i++) {
                    overrides.put(i, regex(args.get(i)));
                }
            } else {
                unknown("Expected minimum, field, and regex arguments for " + name);
            }
        } else if ("filter:compare".equals(name)) {
            if (args.size() == 4) {
                String operator = string(args.get(1));
                String mode = string(args.get(2));
                operator = "=".equals(operator) ? "==" : operator;
                mode = mode == null ? null : mode.toUpperCase(Locale.ROOT);
                if (operator != null && Set.of("==", "!=", "<", "<=", ">", ">=").contains(operator)
                                && Set.of("ANY", "ALL").contains(mode == null ? "" : mode)) {
                    overrides.put(1, Parts.literal("OPERATOR:" + operator));
                    overrides.put(2, Parts.literal("MODE:" + mode));
                    protections.add("COMPARE:" + operator + ":" + mode);
                } else {
                    unknown("Unknown filter:compare operator or mode");
                }
            } else {
                unknown("Expected four arguments for filter:compare");
            }
        }

        if ("content".equals(function.namespace())) {
            int offset = -1;
            for (int i = 0; i < args.size(); i++) {
                if ("termOffsetMap".equals(fieldName(unwrap(args.get(i))))) {
                    offset = i;
                    overrides.put(i, Parts.literal("VARIABLE:termOffsetMap"));
                }
            }
            if (Set.of("within", "phrase", "adjacent").contains(function.name())) {
                boolean within = "within".equals(function.name());
                int unzonedOffset = within ? 1 : 0;
                if ((offset != unzonedOffset && offset != unzonedOffset + 1) || offset == args.size() - 1) {
                    unknown("Unknown argument roles for " + name);
                } else {
                    protections.add("CONTENT_TERMS:" + (args.size() - offset - 1));
                    if (within) {
                        overrides.put(offset - 1, numericControl(args.get(offset - 1), name + ":distance", context));
                    }
                    if (offset == unzonedOffset + 1 && string(args.get(0)) != null) {
                        String zone = string(args.get(0));
                        fields.add(zone);
                        fieldUses++;
                        bindings.add(QueryFingerprint.encode("binding", List.of(path(context, label + ":0"), zone)));
                        overrides.put(0, new Parts("FIELD:" + zone, "FIELD"));
                    }
                }
            }
        }

        List<Parts> children = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            String argumentContext = path(context, label + ":" + i);
            topology.add(argumentContext);
            Parts override = overrides.get(i);
            if (override != null) {
                protections.add(override.profile);
            }
            children.add(override != null ? override : visit(args.get(i), argumentContext, depth));
        }
        return combine(label, children, false);
    }

    private Parts numericControl(JexlNode node, String name, String context) {
        BigDecimal value = number(unwrap(node));
        if (value == null || value.signum() < 0 || !Double.isFinite(value.doubleValue())) {
            unknown("Expected a nonnegative numeric control for " + name);
            return Parts.literal("UNKNOWN_CONTROL:" + name);
        }
        add("controlMagnitude", Math.log1p(value.doubleValue()));
        int bucket = value.signum() == 0 ? 0 : 1 + (int) Math.floor(Math.log1p(value.doubleValue()) / Math.log(2));
        counts.merge(path(context, "CONTROL:" + name + ":" + bucket), 1, Integer::sum);
        return new Parts("CONTROL:" + name + ":" + value.stripTrailingZeros().toPlainString(), "CONTROL:" + name);
    }

    private Parts regex(JexlNode node) {
        String value = string(node);
        if (value == null) {
            unknown("Regex argument is not a string literal");
            return Parts.literal("REGEX:UNKNOWN");
        }
        try {
            Pattern.compile(value);
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(value);
            String mode = !analyzer.hasWildCard() ? "LITERAL"
                            : analyzer.isLeadingLiteral() ? (analyzer.isTrailingLiteral() ? "BOTH" : "PREFIX")
                                            : analyzer.isTrailingLiteral() ? "SUFFIX" : "UNANCHORED";
            int prefix = analyzer.getLeadingLiteral() == null ? 0 : analyzer.getLeadingLiteral().length();
            int suffix = analyzer.getTrailingLiteral() == null ? 0 : analyzer.getTrailingLiteral().length();
            add("regexPrefixLength", prefix);
            add("regexSuffixLength", suffix);
            protections.add("REGEX:" + mode);
            return new Parts("REGEX:" + mode + ":" + prefix + ":" + suffix, "REGEX:" + mode);
        } catch (JavaRegexParseException | PatternSyntaxException e) {
            unknown("Unable to classify regex: " + e.getClass().getSimpleName());
            return Parts.literal("REGEX:UNKNOWN");
        }
    }

    private void unknown(String diagnostic) {
        diagnostics.add(diagnostic);
    }

    private void record(String context, String label) {
        String token = path(context, label);
        counts.merge(token, 1, Integer::sum);
        topology.add(token);
        protections.add(label);
    }

    private void add(String name, double value) {
        measurements.merge(name, value, Double::sum);
    }

    private void maximum(String name, double value) {
        measurements.merge(name, value, Math::max);
    }

    private String path(String context, String label) {
        return QueryFingerprint.encode(context, List.of(label));
    }

    private Parts combine(String label, List<Parts> parts, boolean junction) {
        List<String> exact = new ArrayList<>();
        List<String> profiles = new ArrayList<>();
        for (Parts part : parts) {
            exact.add(part.exact);
            profiles.add(part.profile);
        }
        if (junction) {
            Collections.sort(exact);
            profiles = new ArrayList<>(new TreeSet<>(profiles));
        }
        return new Parts(QueryFingerprint.encode(label, exact), QueryFingerprint.encode(label, profiles));
    }

    private void flatten(JexlNode original, Class<?> type, List<JexlNode> result) {
        Deque<JexlNode> pending = new ArrayDeque<>();
        pending.push(original);
        while (!pending.isEmpty()) {
            JexlNode node = unwrap(pending.pop());
            if (node.getClass() == type && !QueryPropertyMarker.findInstance(node).isAnyType()) {
                // Reverse the push order to retain left-to-right traversal without recursing through wide junctions.
                for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
                    pending.push(node.jjtGetChild(i));
                }
            } else {
                result.add(node);
            }
        }
    }

    private static JexlNode unwrap(JexlNode node) {
        while ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1) {
            node = node.jjtGetChild(0);
        }
        return node;
    }

    private static String fieldName(JexlNode node) {
        if (node instanceof ASTIdentifier) {
            return ((ASTIdentifier) node).getName();
        }
        if (node instanceof ASTReference && node.jjtGetNumChildren() > 1 && node.jjtGetChild(0) instanceof ASTIdentifier) {
            StringBuilder field = new StringBuilder(((ASTIdentifier) node.jjtGetChild(0)).getName());
            for (int i = 1; i < node.jjtGetNumChildren(); i++) {
                if (!(node.jjtGetChild(i) instanceof ASTIdentifierAccess)) {
                    return null;
                }
                field.append('.').append(((ASTIdentifierAccess) node.jjtGetChild(i)).getName());
            }
            return field.toString();
        }
        return null;
    }

    private static String string(JexlNode node) {
        node = unwrap(node);
        return node instanceof ASTStringLiteral ? ((ASTStringLiteral) node).getLiteral() : null;
    }

    private static BigDecimal number(JexlNode node) {
        if (node instanceof ASTNumberLiteral) {
            try {
                return new BigDecimal(((ASTNumberLiteral) node).getLiteral().toString());
            } catch (NumberFormatException e) {
                return null; // JEXL permits floating literals that overflow to infinity.
            }
        }
        if ((node instanceof ASTUnaryMinusNode || node instanceof ASTUnaryPlusNode) && node.jjtGetNumChildren() == 1) {
            BigDecimal child = number(unwrap(node.jjtGetChild(0)));
            return child == null ? null : node instanceof ASTUnaryMinusNode ? child.negate() : child;
        }
        return null;
    }

    private static boolean isSignedNumber(JexlNode node) {
        return (node instanceof ASTUnaryMinusNode || node instanceof ASTUnaryPlusNode) && node.jjtGetNumChildren() == 1
                        && unwrap(node.jjtGetChild(0)) instanceof ASTNumberLiteral;
    }

    private static final class Parts {
        private final String exact;
        private final String profile;

        private Parts(String exact, String profile) {
            this.exact = exact;
            this.profile = profile;
        }

        private static Parts literal(String value) {
            return new Parts(value, value);
        }
    }
}
