package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.language.functions.jexl.Exclude;
import datawave.query.language.functions.jexl.Include;

/**
 * A {@link BaseVisitor} implementation that will check any {@code #INCLUDE} or {@code #EXCLUDE} functions in a query for invalid arguments.
 */
public class InvalidIncludeExcludeArgsVisitor extends BaseVisitor {

    private static final String OR = "or";
    private static final String AND = "and";

    public enum REASON {
        /**
         * No arguments were supplied for the function, e.g. {@code #INCLUDE()}.
         */
        NO_ARGS,
        /**
         * Uneven field/value pairings were supplied for the function, e.g. {@code #INCLUDE(FIELD1, value, FIELD2)}.
         */
        UNEVEN_ARGS,
        /**
         * The first argument was "or" or "and", and no field/value pairs were supplied afterwards, e.g. {@code #INCLUDE(OR)}.
         */
        NO_ARGS_AFTER_BOOLEAN,
        /**
         * The first argument was "or" or "and", and uneven field/value pairings were supplied afterwards, e.g. {@code #INCLUDE(OR, value)}.
         */
        UNEVEN_ARGS_AFTER_BOOLEAN
    }

    public static List<InvalidFunction> check(QueryNode node) {
        InvalidIncludeExcludeArgsVisitor visitor = new InvalidIncludeExcludeArgsVisitor();
        List<InvalidFunction> invalidFunctions = new ArrayList<>();
        visitor.visit(node, invalidFunctions);
        return invalidFunctions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        String name = node.getFunction();
        if (name.equalsIgnoreCase(Include.FUNCTION_NAME) || name.equalsIgnoreCase(Exclude.FUNCTION_NAME)) {
            List<String> args = node.getParameterList();
            if (!args.isEmpty()) {
                String firstArg = args.get(0);
                // The first argument is a boolean.
                if (firstArg.equalsIgnoreCase(OR) || firstArg.equalsIgnoreCase(AND)) {
                    // No arguments were supplied after the boolean.
                    if (args.size() == 1) {
                        ((List<InvalidFunction>) data).add(new InvalidFunction(name, args, REASON.NO_ARGS_AFTER_BOOLEAN));
                        // Uneven field/value pairs were supplied after the boolean.
                    } else if (args.size() % 2 == 0) {
                        ((List<InvalidFunction>) data).add(new InvalidFunction(name, args, REASON.UNEVEN_ARGS_AFTER_BOOLEAN));
                    }
                } else if (args.size() % 2 == 1) {
                    // Uneven field/value pairs were supplied.
                    ((List<InvalidFunction>) data).add(new InvalidFunction(name, args, REASON.UNEVEN_ARGS));
                }
            } else {
                // No arguments were supplied. Currently the AccumuloSyntaxParser throws an exception when attempting to parse #INCLUDE() or #EXCLUDE(), so
                // theoretically the args should never be empty. Put here in case that ever changes.
                ((List<InvalidFunction>) data).add(new InvalidFunction(name, args, REASON.NO_ARGS));
            }
        }
        return data;
    }

    public static class InvalidFunction {
        private final String name;
        private final List<String> args;
        private final REASON reason;

        public InvalidFunction(String name, List<String> args, REASON reason) {
            this.name = name;
            this.args = args;
            this.reason = reason;
        }

        public String getName() {
            return name;
        }

        public List<String> getArgs() {
            return args;
        }

        public REASON getReason() {
            return reason;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            InvalidFunction that = (InvalidFunction) object;
            return Objects.equals(name, that.name) && Objects.equals(args, that.args) && reason == that.reason;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, args, reason);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", InvalidFunction.class.getSimpleName() + "[", "]").add("name='" + name + "'").add("args=" + args)
                            .add("reason=" + reason).toString();
        }
    }
}
