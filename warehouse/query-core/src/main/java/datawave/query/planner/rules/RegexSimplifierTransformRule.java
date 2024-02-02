package datawave.query.planner.rules;

/**
 * This class is intended to simplify when multiple .* or .*? are consecutively placed in a regex. The reason we need to simplify this is because there is an
 * exponential increase in the time to match relative to the number of consecutive .* or .*? and to the size of the value. This has been witnessed to take over
 * many days to complete with 8 consecutive .*? and a value length on the order of 1K+.
 */
public class RegexSimplifierTransformRule extends RegexReplacementTransformRule {
    private static final String PATTERN = "(\\.\\*\\??){2,}";
    private static final String REPLACEMENT = ".*?";

    public RegexSimplifierTransformRule() {
        super(PATTERN, REPLACEMENT);
    }

}
