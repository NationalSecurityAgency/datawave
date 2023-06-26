package datawave.query.postprocessing.tf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;

class ContentFunctionArguments {
    private List<String> terms;
    private int distance;
    private float minScore;
    private List<String> zone = null;

    public List<String> terms() {
        return terms;
    }

    public int distance() {
        return distance;
    }

    public List<String> zone() {
        return zone;
    }

    public ContentFunctionArguments(Function f) throws ParseException {
        String functionName = f.name();
        List<JexlNode> args = f.args();

        int currentArg = 0;

        // Pass back the necessary elements based on the function used
        if (functionName.equals(Constants.CONTENT_WITHIN_FUNCTION_NAME)) {
            // if the first argument is an int, then we have no zone provided
            try {
                // integer argument is stored at the root level
                distance = ((Number) JexlNodes.getImage(JexlASTHelper.dereference(args.get(currentArg)))).intValue();
                // Don't want to do the inline ++ in case currentArg still gets
                // incremented on exception
                currentArg++;
            } catch (NumberFormatException | ClassCastException e) {
                // If the first arg isn't an int, then it's the zone
                zone = constructZone(args.get(currentArg++));

                try {
                    // integer argument is stored at the root level
                    distance = ((Number) JexlNodes.getImage(JexlASTHelper.dereference(args.get(currentArg++)))).intValue();
                } catch (NumberFormatException e1) {
                    throw new ParseException("Could not parse an integer distance value");
                }
            }

            // Ensure the next term is the termOffsetMap variable
            if (args.get(currentArg++) == null) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments: " + f.name() + " '"
                                + args.get(currentArg - 1) + "'");
            }

            terms = new ArrayList<>(args.size() - currentArg);

            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(i)))).trim();

                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }

                terms.add(term);
            }
        } else if (functionName.equals(Constants.CONTENT_SCORED_PHRASE_FUNCTION_NAME)) {
            // if the first argument is a float, then we have no zone provided
            Float minScoreArg = readMinScore(getValue(args.get(currentArg)));
            if (null != minScoreArg) {
                minScore = minScoreArg.floatValue();
                // Don't want to do the inline ++ in case currentArg still gets
                // incremented on exception
                currentArg++;
            } else {
                // If the first arg isn't an int, then it's the zone
                zone = constructZone(args.get(currentArg++));

                minScoreArg = readMinScore(getValue(args.get(currentArg)));
                if (minScoreArg == null) {
                    throw new ParseException("Could not parse a float min score value");
                } else {
                    minScore = minScoreArg.floatValue();
                }
            }

            // Ensure the next term is the termOffsetMap variable
            if (args.get(currentArg++) == null) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments: " + f.name() + " '"
                                + args.get(currentArg - 1) + "'");
            }

            terms = new ArrayList<>(args.size() - currentArg);

            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(i)))).trim();

                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }

                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_ADJACENT_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone adjacent function
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME
                            .equalsIgnoreCase(String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(currentArg)))))) {
                zone = constructZone(args.get(currentArg++));
            }

            // Ensure the next term is the termOffsetMap variable
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME
                            .equalsIgnoreCase(String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(currentArg++)))).trim())) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }

            // Get the actual terms
            terms = new ArrayList<>(args.size() - currentArg);

            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(i)))).trim();

                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }

                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_PHRASE_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone phrase function
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME
                            .equalsIgnoreCase(String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(currentArg)))))) {
                zone = constructZone(args.get(currentArg++));
                if (zone.isEmpty()) {
                    currentArg--;
                }
            }

            String nextArg = getValue(args.get(currentArg++));

            // Ensure the next term is the termOffsetMap variable
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME.equalsIgnoreCase(nextArg)) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }

            readTerms(args, currentArg);
        } else {
            throw new ParseException("Unrecognized content function: " + f.name());
        }
    }

    // this is added to fix the case where a UnaryMinusNode returns a null image
    private String getValue(JexlNode arg) {
        return JexlStringBuildingVisitor.buildQuery(JexlASTHelper.dereference(arg)).trim();
    }

    private List<String> constructZone(JexlNode node) {
        List<String> zones = new ArrayList<>();

        for (String zone : JexlASTHelper.getIdentifierNames(node)) {
            if (zone.length() > 1 && zone.charAt(0) == '\'' && zone.charAt(zone.length() - 1) == '\'') {
                zones.add(JexlASTHelper.deconstructIdentifier(zone.substring(1, zone.length() - 1)));
            } else {
                zones.add(JexlASTHelper.deconstructIdentifier(zone));
            }
        }
        return zones;
    }

    private void readTerms(final List<JexlNode> args, int currentArg) {
        // Get the actual terms
        terms = new ArrayList<>(args.size() - currentArg);

        // Get the actual terms
        for (int i = currentArg; i < args.size(); i++) {
            String term = String.valueOf(JexlNodes.getImage(JexlASTHelper.dereference(args.get(i)))).trim();

            if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                term = term.substring(1, term.length() - 1);
            }

            terms.add(term.toLowerCase(Locale.ENGLISH));
        }
    }

    private static final Float readMinScore(String arg) {
        try {
            return Float.parseFloat(arg);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }
}
