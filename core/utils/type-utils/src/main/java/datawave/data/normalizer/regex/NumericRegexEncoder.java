package datawave.data.normalizer.regex;

import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.google.common.base.CharMatcher;

import datawave.data.normalizer.ZeroRegexStatus;
import datawave.data.normalizer.regex.visitor.AlternationDeduper;
import datawave.data.normalizer.regex.visitor.AnchorTrimmer;
import datawave.data.normalizer.regex.visitor.DecimalPointPlacer;
import datawave.data.normalizer.regex.visitor.DecimalPointValidator;
import datawave.data.normalizer.regex.visitor.EmptyLeafTrimmer;
import datawave.data.normalizer.regex.visitor.ExponentialBinAdder;
import datawave.data.normalizer.regex.visitor.NegativeNumberPatternInverter;
import datawave.data.normalizer.regex.visitor.NegativeVariantExpander;
import datawave.data.normalizer.regex.visitor.NonEncodedNumbersChecker;
import datawave.data.normalizer.regex.visitor.NumericCharClassValidator;
import datawave.data.normalizer.regex.visitor.OptionalVariantExpander;
import datawave.data.normalizer.regex.visitor.PrintVisitor;
import datawave.data.normalizer.regex.visitor.SimpleNumberEncoder;
import datawave.data.normalizer.regex.visitor.StringVisitor;
import datawave.data.normalizer.regex.visitor.ZeroLengthRepetitionTrimmer;
import datawave.data.normalizer.regex.visitor.ZeroTrimmer;
import datawave.data.normalizer.regex.visitor.ZeroValueNormalizer;
import datawave.data.type.util.NumericalEncoder;

/**
 * This class handles provides functionality for encoding numeric regexes that are meant to match against numbers that were previously encoded via
 * {@link NumericalEncoder#encode(String)}. It is expected that incoming regexes are initially written to match against base ten numbers. Due to the complex
 * nature of how numbers are encoded and trimmed, accuracy is NOT guaranteed when using this class to encode numeric regexes.
 * <P>
 * <P>
 * <strong>Requirements</strong>
 * <P>
 * The following requirements apply to all incoming regexes:
 * <ul>
 * <li>Patterns may not be blank.</li>
 * <li>Patterns may not contain whitespace.</li>
 * <li>Patterns must be compilable.</li>
 * <li>Patterns may not contain any letters other than {@code "\d"}.</li>
 * <li>Patterns may not contain any escaped characters other than {@code "\."}, {@code "\-"}, or {@code "\d"}.</li>
 * <li>Patterns may not contain any groups, e.g. {@code "(45.*)"}.</li>
 * <li>Patterns may not contain any decimal points that are followed by {@code ?} {@code *} {@code +} or a repetition quantifier such as {@code {3}}.</li>
 * </ul>
 * <P>
 * <P>
 * <strong>Supported Regex Features</strong>
 * <P>
 * The following regex features are supported, with any noted caveats.
 * <ul>
 * <li>Wildcards {@code "."}.</li>
 * <li>Digit character class {@code "\d"}.</li>
 * <li>Character class lists {@code "[]"}. CAVEAT: Digit characters only. Ranges are supported.</li>
 * <li>Zero or more quantifier {@code "*"}.</li>
 * <li>One or more quantifier {@code "+"}.</li>
 * <li>Repetition quantifier {@code "{x}"}, {@code "{x,}"}, and {@code "{x,y}"}.</li>
 * <li>Anchors {@code "^"} and {@code "$"}. CAVEAT: Technically not truly supported as they are ultimately removed during the pre-optimization process. However,
 * using them will not result in an error.</li>
 * <li>Alternations {@code "|"}.</li>
 * </ul>
 * Additionally, in order to mark a regex pattern as intended to match negative numbers only, a minus sign should be placed at the beginning of the regex
 * pattern, e.g. {@code "-34.*"}, or at the beginning of each desired alternated pattern.
 * <P>
 * <P>
 * <strong>Optimizations</strong>
 * <P>
 * Before encoding the incoming regex, it will undergo the following modifications to optimize the ease of encoding:
 * <ol>
 * <li>Any empty alternations will be removed.</li>
 * <li>Any occurrences of the anchors {@code ^} or {@code $} will be removed. These will need to be added back into the returned encoded regex pattern
 * afterwards if desired.</li>
 * <li>Optional variants (characters followed by {@code ?}} will be expanded into additional alternations as seen. This will not apply to any {@code ?}
 * instances that directly follow a {@code *}, {@code +}, or {@code {x}}, as the {@code ?} in this case modifies the greediness of the matching rather than
 * whether or not a character can be present.</li>
 * <li>Any characters immediately followed by the repetition quantifier {@code "{0}"} or {@code "{0,0}"} will be removed as they are expected to occur zero
 * times. This does not apply to characters with the repetition quantifier {@code "{0,}"} or a variation of {@code "{0,x}"}.</li>
 * <li>Any patterns starting with {@code ".*"} or {@code ".+"} will result in the addition of an alternation of the same pattern with a minus sign in front of
 * it to ensure a variant for matching negative numbers is added. This does not apply to any regex patterns already starting with {@code "-.*"} or
 * {@code "-.+"}.</li>
 * <li>In some cases a pattern may match both exactly zero and another number greater than one, e.g. the pattern "[0-9].*". In this case, an alternation for the
 * character {@code "0"} will be added (i.e. {@code "[0-9].*|0"}) to ensure that the ability to match zero is not lost when enriching the pattern with the
 * required exponential bins to target the appropriate encoded numbers.</li>
 * <li>Pattern alternations will be de-duped.</li>
 * </ol>
 * <P>
 * <P>
 * A strong effort has been made to make resulting encoded patterns as accurate as possible, but there is always a chance of at least some inaccuracy, given the
 * nature of how numbers are encoded, particularly when it comes to numbers that are very similar other than the location of a decimal point, if present, in
 * them. If you find that the resulting encoded regex is not matching the desired encoding numbers, try to simplify it into a higher number of alternations with
 * simpler regexes if possible.
 * 
 * @see NumericalEncoder
 */
public class NumericRegexEncoder {
    
    private static final Logger log = Logger.getLogger(NumericRegexEncoder.class);
    
    /**
     * Matches against any unescaped d characters, and any other letters. If \d is present, that indicates a digit and is allowed.
     */
    private static final Pattern RESTRICTED_LETTERS_PATTERN = Pattern.compile(".*[a-ce-zA-Z].*");
    
    /**
     * Matches any escaped character that is not \. \- or \d.
     */
    private static final Pattern RESTRICTED_ESCAPED_CHARS_PATTERN = Pattern.compile(".*\\\\[^.d\\-].*");
    
    /**
     * Matches any regex that consists only of anchors, hyphens (escaped or not), escaped periods, repetitions, the quantifier *, the quantifier +, optionals,
     * alternations, and groups in any order with no alphanumeric characters that give any meaningful numeric information.
     */
    private static final Pattern NONSENSE_PATTERN = Pattern.compile("^\\^?(\\(*(\\\\\\.)*\\)*|(\\(*\\\\?[\\-*+?|])*\\)*|(\\{.*}))*\\$?$");
    
    /**
     * Matches any decimal points with ? + * or a repetition quantifier directly following them.
     */
    private static final Pattern INVALID_DECIMAL_POINTS_PATTERN = Pattern.compile(".*\\\\\\.[?+*{].*");
    
    /**
     * Matches against any variation of {@code .*}, {@code .+}, {@code .*?}, {@code .+?} that may or may not repeat, and that may or may not contain start
     * and/or end anchors.
     */
    private static final Pattern NORMALIZATION_NOT_REQUIRED_PATTERN = Pattern.compile("^\\^?(\\.[*+]\\??)+\\$?$");
    
    /**
     * Encode the given numeric regex pattern such that it will match against encoded numbers.
     * 
     * @param regex
     *            the regex pattern
     * @return the encoded regex pattern
     */
    public static String encode(String regex) {
        return new NumericRegexEncoder(regex).encode();
    }
    
    private final String pattern;
    private Node patternTree;
    
    private NumericRegexEncoder(String pattern) {
        this.pattern = pattern;
    }
    
    public static ZeroRegexStatus getZeroRegexStatus(String regex) {
        return ZeroTrimmer.getStatus(RegexParser.parse(regex).getChildren());
    }
    
    private String encode() {
        if (log.isDebugEnabled()) {
            log.debug("Encoding pattern " + pattern);
        }
        
        // Check the pattern for any quick failures.
        checkPatternForQuickFailures();
        // Encode the pattern only if it requires it.
        if (isEncodingRequired()) {
            parsePatternTree();
            normalizePatternTree();
            encodePatternTree();
            
            if (log.isDebugEnabled()) {
                log.debug("Encoded pattern '" + pattern + "' to '" + StringVisitor.toString(this.patternTree) + "'");
            }
            
            return StringVisitor.toString(this.patternTree);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Encoding not required for pattern '" + pattern + "'");
            }
            return this.pattern;
        }
    }
    
    /**
     * Pre-validate the regex to quickly identify any indications that the regex is not valid for numerical expansion.
     */
    private void checkPatternForQuickFailures() {
        checkForBlankPattern();
        checkForWhitespace();
        checkForCompilation();
        checkForNonsense();
        checkForRestrictedLetters();
        checkForRestrictedEscapedCharacters();
        checkForGroups();
        checkForQuantifiedDecimalPoints();
    }
    
    /**
     * Throws an exception if the regex pattern is blank.
     */
    private void checkForBlankPattern() {
        if (this.pattern.isEmpty()) {
            throw new IllegalArgumentException("Regex pattern may not be blank.");
        }
    }
    
    /**
     * Throws an exception if the regex contains any whitespace.
     */
    private void checkForWhitespace() {
        if (CharMatcher.whitespace().matchesAnyOf(pattern)) {
            throw new IllegalArgumentException("Regex pattern may not contain any whitespace.");
        }
    }
    
    /**
     * Throws an exception if the regex cannot be compiled.
     */
    private void checkForCompilation() {
        try {
            Pattern.compile(this.pattern);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Regex pattern will not compile.", e);
        }
    }
    
    private void checkForNonsense() {
        if (NONSENSE_PATTERN.matcher(this.pattern).matches()) {
            throw new IllegalArgumentException("A nonsense pattern has been given that cannot be normalized.");
        }
    }
    
    /**
     * Throws an exception if the regex contains any letter other than an escaped lowercase d.
     */
    private void checkForRestrictedLetters() {
        if (RESTRICTED_LETTERS_PATTERN.matcher(pattern).matches() || containsUnescapedLowercaseD()) {
            throw new IllegalArgumentException(
                            "Regex pattern may not contain any letters other than \\d to indicate a member of the digit character class 0-9.");
        }
    }
    
    /**
     * Return whether the regex contains an unescaped d.
     */
    private boolean containsUnescapedLowercaseD() {
        int pos = pattern.indexOf(RegexConstants.LOWERCASE_D);
        while (pos != -1) {
            if (pos == 0 || pattern.charAt(pos - 1) != RegexConstants.BACKSLASH) {
                return true;
            }
            pos = pattern.indexOf(RegexConstants.LOWERCASE_D, pos + 1);
        }
        return false;
    }
    
    /**
     * Throws an exception if the regex contains any escaped characters other than {@code \.}, {@code \-} or {@code \d}.
     */
    private void checkForRestrictedEscapedCharacters() {
        if (RESTRICTED_ESCAPED_CHARS_PATTERN.matcher(this.pattern).matches()) {
            throw new IllegalArgumentException("Regex pattern may not contain any escaped characters other than \\. \\- or \\d.");
        }
    }
    
    /**
     * Throws an exception if the regex contains any occurrences of '(' indicating the start of a group.
     */
    private void checkForGroups() {
        if (this.pattern.contains("(")) {
            throw new IllegalArgumentException("Regex pattern may not contain any groups.");
        }
    }
    
    /**
     * Throws an exception if the regex contains any decimal points directly followed by * + or {}.
     */
    private void checkForQuantifiedDecimalPoints() {
        if (INVALID_DECIMAL_POINTS_PATTERN.matcher(this.pattern).matches()) {
            throw new IllegalArgumentException("Regex pattern may not contain any decimal points that are directly followed by * ? or {}.");
        }
    }
    
    /**
     * Returns whether the regex requires normalization.
     *
     * @return true if the regex requires normalization, or false otherwise.
     */
    private boolean isEncodingRequired() {
        return !NORMALIZATION_NOT_REQUIRED_PATTERN.matcher(this.pattern).matches();
    }
    
    /**
     * Parse the regex to a node tree.
     */
    private void parsePatternTree() {
        parsePatternToTree();
        validateCharClasses();
        validateDecimalPoints();
    }
    
    /**
     * Normalize the pattern tree.
     */
    private void normalizePatternTree() {
        trimAnchors();
        trimZeroLengthRepetitions();
        trimEmptyLeafs();
        expandOptionalVariants();
        expandNegativeVariants();
        expandZeroValues();
    }
    
    /**
     * Encode the pattern tree.
     */
    private void encodePatternTree() {
        dedupe();
        encodeSimpleNumbers();
        // If there are no more unencoded sub-patterns in the tree after encoding simple numbers, no further work needs to be done.
        if (!moreToEncode()) {
            return;
        }
        addExponentialBins();
        trimZeros();
        invertNegativePatterns();
        addDecimalPoints();
        dedupe();
    }
    
    /**
     * Parse the pattern to a node tree.
     */
    private void parsePatternToTree() {
        this.patternTree = RegexParser.parse(this.pattern);
        
        if (log.isDebugEnabled()) {
            log.debug("Parsed pattern to tree structure:\n" + PrintVisitor.printToString(this.patternTree));
        }
    }
    
    /**
     * Verify that the regex pattern does not contain any character classes with characters other than digits or a period.
     */
    private void validateCharClasses() {
        NumericCharClassValidator.validate(this.patternTree);
        
        if (log.isDebugEnabled()) {
            log.debug("Validated character classes in regex");
        }
    }
    
    /**
     * Verify that the regex pattern does not contain any alternated expressions that have more than one required decimal point.
     */
    private void validateDecimalPoints() {
        DecimalPointValidator.validate(this.patternTree);
        
        if (log.isDebugEnabled()) {
            log.debug("Validated decimal points classes in regex");
        }
    }
    
    /**
     * Trim all anchors.
     */
    private void trimAnchors() {
        updatePatternTree(AnchorTrimmer::trim, "trimming anchors");
    }
    
    /**
     * Trim all elements that occur exactly zero times.
     */
    private void trimZeroLengthRepetitions() {
        updatePatternTree(ZeroLengthRepetitionTrimmer::trim, "trimming zero-length repetition characters");
        
        // If the pattern is empty afterwards, throw an exception.
        if (this.patternTree == null) {
            throw new IllegalArgumentException("Regex pattern is empty after trimming all characters followed by {0} or {0,0}.");
        }
    }
    
    /**
     * Trim the tree of any empty nodes and empty alternations, and verify if we still have a pattern to encode.
     */
    private void trimEmptyLeafs() {
        updatePatternTree(EmptyLeafTrimmer::trim, "trimming empty leafs");
    }
    
    /**
     * Expand optional variants.
     */
    private void expandOptionalVariants() {
        updatePatternTree(OptionalVariantExpander::expand, "expanding optional variants");
    }
    
    /**
     * Expand any patterns beginning with {@code .} to include a version with a minus sign in front of it.
     */
    private void expandNegativeVariants() {
        updatePatternTree(NegativeVariantExpander::expand, "expanding negative variants");
    }
    
    /**
     * If any patterns can match the number '0', add an alternation with '0'.
     */
    private void expandZeroValues() {
        updatePatternTree(ZeroValueNormalizer::expand, "normalizing zero-value characters");
    }
    
    /**
     * Remove any duplicate alternations.
     */
    private void dedupe() {
        updatePatternTree(AlternationDeduper::dedupe, "de-duping");
    }
    
    /**
     * Encode any and all simple numbers present in the pattern.
     */
    private void encodeSimpleNumbers() {
        updatePatternTree(SimpleNumberEncoder::encode, "encoding simple numbers");
    }
    
    /**
     * Return whether there are unencoded sub-patterns in the tree after encoding simple numbers.
     * 
     * @return true if there are more patterns to encode, or false otherwise
     */
    private boolean moreToEncode() {
        return NonEncodedNumbersChecker.check(this.patternTree);
    }
    
    /**
     * Add exponential bin range information, e.g. \+[a-z], ![A-Z], etc.
     */
    private void addExponentialBins() {
        updatePatternTree(ExponentialBinAdder::addBins, "adding exponential bin information");
    }
    
    /**
     * Trim/consolidate any leading zeros in partially-encoded patterns.
     */
    private void trimZeros() {
        updatePatternTree(ZeroTrimmer::trim, "trimming leading/trailing zeros");
    }
    
    /**
     * Invert any patterns that are meant to match negative numbers.
     */
    private void invertNegativePatterns() {
        updatePatternTree(NegativeNumberPatternInverter::invert, "inverting patterns for negative numbers");
    }
    
    /**
     * Add decimal points where required.
     */
    private void addDecimalPoints() {
        updatePatternTree(DecimalPointPlacer::addDecimalPoints, "adding decimal points");
    }
    
    private void updatePatternTree(Function<Node,Node> function, String operationDescription) {
        this.patternTree = function.apply(this.patternTree);
        
        if (log.isDebugEnabled()) {
            log.debug("Regex after " + operationDescription + ": " + StringVisitor.toString(this.patternTree));
        }
    }
}
