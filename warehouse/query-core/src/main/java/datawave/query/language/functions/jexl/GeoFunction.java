package datawave.query.language.functions.jexl;

import java.util.ArrayList;
import java.util.Set;

import datawave.query.language.functions.QueryFunction;

import com.google.common.collect.Sets;

public class GeoFunction extends JexlQueryFunction {
    private static final Set<String> COMMANDS = Sets.newHashSet("bounding_box", "circle");
    private boolean[] shouldQuote = null;

    public GeoFunction() {
        super("geo", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        int numParams = parameterList.size();

        if (numParams == 0 || !COMMANDS.contains(parameterList.get(0).toLowerCase())) {
            throw new IllegalArgumentException("First geo function argument must be one of " + COMMANDS);
        }

        String function = parameterList.get(0);
        if (function.equalsIgnoreCase("bounding_box")) {
            switch (numParams) {
                case 4:
                    shouldQuote = new boolean[] {false, false, true, true};
                    break;
                case 7:
                    shouldQuote = new boolean[] {false, false, false, true, true, true, true};
                    break;
                default:
                    throw new IllegalArgumentException("Geo bounding_box function requires either 4 or 7 arguments; " + parameterList.size() + " provided.");
            }
        }
        if (function.equalsIgnoreCase("circle")) {
            switch (numParams) {
                case 4:
                    shouldQuote = new boolean[] {false, false, true, false};
                    break;
                default:
                    throw new IllegalArgumentException("Geo bounding_box function requires 4 arguments; " + parameterList.size() + " provided.");
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder f = new StringBuilder(64);
        f.append("geo:within_");

        String functionName = parameterList.get(0).toLowerCase();
        f.append(shouldQuote[0] == true ? escapeString(functionName) : functionName);
        f.append('(');

        for (int x = 1; x < parameterList.size(); x++) {
            String p = parameterList.get(x);
            f.append(shouldQuote[x] == true ? escapeString(p) : p);
            f.append(", ");
        }
        f.setLength(f.length() - ", ".length());
        f.append(')');
        return f.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new GeoFunction();
    }

}
