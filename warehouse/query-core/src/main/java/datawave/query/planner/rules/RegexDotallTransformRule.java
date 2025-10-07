package datawave.query.planner.rules;

/**
 * This class is intended to simplify the (\s|.) with just a . since we have added the DOTALL flag to the java Pattern compilation of regexes (@see
 * datawave.query.jexl.JexlPatternCache).
 */
public class RegexDotallTransformRule extends RegexReplacementTransformRule {
    private static final String PATTERN = "\\((\\\\s\\|\\.|\\.\\|\\\\s)\\)";
    private static final String REPLACEMENT = ".";

    public RegexDotallTransformRule() {
        super(PATTERN, REPLACEMENT);
    }
}
