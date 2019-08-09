package datawave.query.parser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * A class used to analyze and manipulate regular expressions
 * 
 * TODO: if somebody finds a usable java Pattern grammar, please rewrite this class
 */
public class JavaRegexAnalyzer {
    protected static final Logger log = Logger.getLogger(JavaRegexAnalyzer.class);
    
    // Types as applied to portions of the regex. We are interested in portions that
    // are literals and those that contain regex constructs.
    private enum RegexType {
        LITERAL(true), // a literal value
        ESCAPED_LITERAL(true), // an escaped literal (e.g. \[ or \.)
        REGEX(false), // a regex
        REGEX_QUANTIFIER(false), // a regex quantifier like * or +
        ESCAPED_REGEX(false), // an escaped regex construct
        IGNORABLE_REGEX(false); // an ignorable regex construct (e.g. boundary or quoting)
        
        private boolean literal = false;
        
        private RegexType(boolean lit) {
            this.literal = lit;
        }
        
        public boolean isLiteral() {
            return this.literal;
        }
    }
    
    private static class RegexPart {
        // the regex is not-final to allow applyRegexCaseSensitivity
        public String regex;
        public RegexType type;
        public final boolean nonCapturing;
        
        public RegexPart(String reg, RegexType typ, boolean nonCapt) {
            this.regex = reg;
            this.type = typ;
            this.nonCapturing = nonCapt;
        }
        
        public RegexPart(String reg, RegexType typ, int nonCapt) {
            this(reg, typ, (nonCapt > 0));
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RegexPart)) {
                return false;
            }
            RegexPart other = (RegexPart) o;
            return regex.equals(other.regex) && type.equals(other.type) && (nonCapturing == other.nonCapturing);
        }
        
        @Override
        public int hashCode() {
            return regex.hashCode() + type.hashCode() + (nonCapturing ? 1 : 0);
        }
        
        @Override
        public String toString() {
            return regex;
        }
    }
    
    public static class JavaRegexParseException extends ParseException {
        private static final long serialVersionUID = -8377431598528407124L;
        
        public JavaRegexParseException(String s, int errorOffset) {
            super(s, errorOffset);
        }
    }
    
    // The regex broken into parts
    private RegexPart[] regexParts = null;
    
    // The updated value portion
    private String leadingLiteral = null;
    private String trailingLiteral = null;
    private boolean updatedLiterals = false;
    
    // do we have a capturing regex somewhere
    private boolean hasWildCard = false;
    
    // the characters that when escaped have special meanings (i.e. not an escaped literal value)
    private static final String ESCAPED_REGEX_CHARS = "0123456789xutnrfaecdDsSwWpPbBAGzZQE";
    
    // Non digit matching regex chars
    private static final String NON_DIGIT_ESCAPED_REGEX_CHARS = "tnrfaecDsw";
    
    // Quoting regex chars
    private static final String QUOTING_REGEX_CHARS = "QE";
    
    // Boundary regex chars
    private static final String BOUNDARY_REGEX_CHARS = "bBAGzZ";
    
    // Boundary chars
    private static final String BOUNDARY_CHARS = "^$";
    
    // digit character classes
    private static final List<String> DIGIT_CHARACTER_CLASSES = Arrays.asList("\\P{Lower}", "\\P{Upper}", "\\p{ASCII}", "\\P{Alpha}", "\\p{Digit}",
                    "\\p{Alnum}", "\\P{{Punct}", "\\p{Graph}", "\\p{Print}", "\\P{Blank}", "\\P{Cntrl}", "\\p{XDigit}", "\\P{Space}", "\\P{javaLowerCase}",
                    "\\P{javaUpperCase}", "\\P{javaWhitespace}", "\\P{javaMirrored}", "\\P{InGreek}", "\\P{Lu}", "\\P{Sc}", "\\p{L}");
    
    // the character class chars
    private static final String CHAR_REGEX_CHARS = "0xutnrfaecdDsSwWpP";
    
    // the back reference chars
    private static final String BACK_REF_CHARS = "123456789";
    
    // characters that are have special meanings
    private static final String RESERVED_CHARS = ".*?+{}^$|()[]";
    
    // Some pattern precompiling
    private static final String FLAG_REGEX = "\\(\\?-?[idmsux]\\).*";
    private static final Pattern flagRegexPattern = Pattern.compile(FLAG_REGEX);
    private static final String NON_CAPTURING_REGEX = "\\(\\?[idmsux:=!>(<[=!]-)].*";
    private static final Pattern nonCapturingPattern = Pattern.compile(NON_CAPTURING_REGEX);
    private static final String CURLY_QUANTIFIER_REGEX = "\\{([0-9]+)(,([0-9]*))?\\}.*";
    private static final Pattern curlyQuantifierPattern = Pattern.compile(CURLY_QUANTIFIER_REGEX);
    
    // characters that serve as quantifiers
    private static final String QUANTIFIERS = "*+?";
    
    private static int MIN_INDEX = 0;
    private static int MAX_INDEX = 1;
    
    private static final RegexPart OPEN_PAREN = new RegexPart("(", RegexType.REGEX, false);
    private static final RegexPart CLOSE_PAREN = new RegexPart(")", RegexType.REGEX, false);
    private static final RegexPart ALTERNATE = new RegexPart("|", RegexType.REGEX, false);
    
    // construct a regex analyzer
    public JavaRegexAnalyzer(String regex) throws JavaRegexParseException {
        setRegex(regex);
    }
    
    public String getRegex() {
        return getRegex(regexParts);
    }
    
    public static String getRegex(RegexPart[] regexParts) {
        StringBuilder regex = new StringBuilder();
        for (RegexPart part : regexParts) {
            regex.append(part.regex);
        }
        return regex.toString();
    }
    
    @Override
    public String toString() {
        return getRegex();
    }
    
    /**
     * Set the regex on this analyzer. This will do the parsing of the regex into its parts up front. Note that this parser only needs to parse enough for the
     * purposes of the applyRegexCaseSensitivity and the determination of the leading and trailing literals.
     * 
     * @param regex
     */
    public void setRegex(String regex) throws JavaRegexParseException {
        regexParts = null;
        List<RegexPart> partList = new ArrayList<>();
        
        // parse on '\' characters,
        // then walk forward from each one to determine the escaped character or character class, parsing on '[' and ']' characters
        // as we go
        // note that things between a \Q and \E do not count as escaped character or character classes.
        String[] parts = Iterables.toArray(Splitter.on('\\').split(regex), String.class);
        
        // is the next section/part escaped
        boolean escaped = false;
        
        // are we in a quoted section (between \\Q and \\E)
        boolean quoted = false;
        
        // keeping track of paren and bracket nesting
        LinkedList<String> parensAndBrackets = new LinkedList<>();
        
        // remember if we are inside any brackets to enable distinguishing between LITERAL and REGEX
        int bracketCount = 0;
        
        // are we in a non-capturing group: we want to hold these as separate entities as they can be ignored with determining the updated value
        int nonCapturing = 0;
        
        // expression is the portion prefaced with a '\\'
        String expression = null;
        
        // remainder is non-escaped portion
        String remainder = null;
        
        // keep track of the column for exceptions
        int column = 0;
        
        // for each part
        for (int i = 0; i < parts.length; i++) {
            // if not an escaped portion, then the entire part is the remainder and the next part is escaped
            if (!escaped) {
                remainder = parts[i];
                escaped = true;
            }
            // if in a quoted section, then end quoting if we find \\E
            else if (quoted) {
                if (parts[i].startsWith("E")) {
                    quoted = false;
                    expression = "E";
                    remainder = parts[i].substring(1);
                } else {
                    expression = "";
                    remainder = parts[i];
                }
            }
            // else in an escaped, non-quoted section
            else {
                // endExpression is the division between the escaped character/class and the remainder
                int endExpression = 0;
                
                // check for \\\\
                if (parts[i].equals("")) {
                    parts[i] = "\\";
                    endExpression = 1;
                    // In this case the next section is not escaped
                    escaped = false;
                }
                // check for a \\p{class} construct
                else if (parts[i].startsWith("p{")) {
                    endExpression = parts[i].indexOf('}');
                    if (endExpression < 0) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\p{... without and end } character: " + parts[i], column);
                    } else {
                        endExpression++;
                    }
                }
                // check for a \\P{class} construct
                else if (parts[i].startsWith("P{")) {
                    endExpression = parts[i].indexOf('}');
                    if (endExpression < 0) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\P{... without and end } character: " + parts[i], column);
                    } else {
                        endExpression++;
                    }
                }
                // check for a \\cX construct
                else if (parts[i].startsWith("c")) {
                    if (parts[i].length() == 1) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\cX without the X character: " + parts[i], column);
                    }
                    endExpression = 2;
                }
                // check for a \\0 (back reference), or \\0n or \\0nn or \\0mnn (octal character)
                else if (parts[i].startsWith("0")) {
                    int maxExpression = Math.min(4, parts[i].length());
                    for (endExpression = 1; endExpression < maxExpression; endExpression++) {
                        try {
                            if (Integer.parseInt(parts[i].substring(1, endExpression + 1), 8) > 255) {
                                break;
                            }
                        } catch (Exception e) {
                            break;
                        }
                    }
                }
                // check for \\xhh
                else if (parts[i].startsWith("x")) {
                    if (parts[i].length() < 3) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\xhh without the hh characters: " + parts[i], column);
                    }
                    endExpression = 3;
                    try {
                        Integer.parseInt(parts[i].substring(1, endExpression), 16);
                    } catch (Exception e) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\xhh without the hh characters: " + parts[i], column);
                    }
                }
                // check for \\uhhhh
                else if (parts[i].startsWith("u")) {
                    if (parts[i].length() < 5) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\uhhhh without the hhhh characters: " + parts[i], column);
                    }
                    endExpression = 5;
                    try {
                        Integer.parseInt(parts[i].substring(1, endExpression), 16);
                    } catch (Exception e) {
                        throw new JavaRegexParseException("Invalid Regular Expression: Found a \\uhhhh without the hh characters: " + parts[i], column);
                    }
                }
                // assume \\?
                else {
                    endExpression = 1;
                }
                
                // now pull off the expression and remainder
                if (endExpression == 0) {
                    remainder = parts[i];
                } else if (endExpression < parts[i].length()) {
                    expression = parts[i].substring(0, endExpression);
                    remainder = parts[i].substring(endExpression);
                } else {
                    expression = parts[i];
                }
            }
            
            if (expression != null) {
                RegexType type = RegexType.ESCAPED_REGEX;
                
                // determine if this is an escaped regex or an escaped literal
                if (expression.length() == 1) {
                    if (ESCAPED_REGEX_CHARS.indexOf(expression.charAt(0)) < 0) {
                        // if we are in a bracket, then its a regex
                        type = (bracketCount > 0 ? RegexType.REGEX : RegexType.ESCAPED_LITERAL);
                    }
                    // check for quoting chars
                    else if (QUOTING_REGEX_CHARS.indexOf(expression.charAt(0)) >= 0) {
                        if (expression.equals("Q")) {
                            quoted = true;
                        }
                        type = RegexType.IGNORABLE_REGEX;
                    }
                    // check for boundary chars
                    else if (BOUNDARY_REGEX_CHARS.indexOf(expression.charAt(0)) >= 0) {
                        type = RegexType.IGNORABLE_REGEX;
                    }
                }
                
                partList.add(new RegexPart("\\" + expression, type, nonCapturing));
                column += expression.length() + 1;
                
                expression = null;
            }
            
            if (remainder != null) {
                if (quoted) {
                    // if we are in a bracket, then its a regex
                    RegexType type = (bracketCount > 0 ? RegexType.REGEX : RegexType.LITERAL);
                    partList.add(new RegexPart(remainder, type, nonCapturing));
                    column += remainder.length();
                } else {
                    // check for () or [] constructs
                    for (int c = 0; c < remainder.length(); c++) {
                        char character = remainder.charAt(c);
                        
                        if (RESERVED_CHARS.indexOf(character) >= 0) {
                            if (character == '(') {
                                // look for a non-capturing group
                                if (c < remainder.length() - 1 && remainder.charAt(c + 1) == '?') {
                                    nonCapturing++;
                                    String value = remainder.substring(c);
                                    // look for a flag
                                    if (flagRegexPattern.matcher(value).matches()) {
                                        int len = (value.charAt(2) == '-' ? 5 : 4);
                                        value = value.substring(0, len);
                                        partList.add(new RegexPart(value, RegexType.REGEX, nonCapturing));
                                        column += value.length();
                                        nonCapturing--;
                                        c += len - 1;
                                    } else if (nonCapturingPattern.matcher(value).matches()) {
                                        int len = (value.charAt(2) == '<' ? 4 : 3);
                                        value = value.substring(0, len);
                                        parensAndBrackets.addLast(value);
                                        partList.add(new RegexPart(value, RegexType.REGEX, nonCapturing));
                                        column += value.length();
                                        c += len - 1;
                                    } else {
                                        throw new JavaRegexParseException(
                                                        "Invalid Regular Expression: does not match a known non-capturing group construct: " + value, column);
                                    }
                                } else {
                                    parensAndBrackets.addLast("(");
                                    // forcing this to non-capturing as it is really not part of the literal value contained therein
                                    partList.add(new RegexPart("(", RegexType.REGEX, nonCapturing));
                                    column += 1;
                                }
                            } else if (character == '[') {
                                bracketCount++;
                                if (c < remainder.length() - 1 && remainder.charAt(c + 1) == '^') {
                                    parensAndBrackets.addLast("[^");
                                    partList.add(new RegexPart("[^", RegexType.REGEX, nonCapturing));
                                    column += 2;
                                    c++;
                                } else {
                                    parensAndBrackets.addLast("[");
                                    partList.add(new RegexPart("[", RegexType.REGEX, nonCapturing));
                                    column += 1;
                                }
                            } else if (character == '{') {
                                String value = remainder.substring(c);
                                // look for a full {n} or {n,m}
                                if (curlyQuantifierPattern.matcher(value).matches()) {
                                    int len = value.indexOf('}') + 1;
                                    value = value.substring(0, len);
                                    partList.add(new RegexPart(value, RegexType.REGEX_QUANTIFIER, nonCapturing));
                                    column += value.length();
                                    c += len - 1;
                                } else {
                                    throw new JavaRegexParseException("Found a {... but expected {n} or {n,} or {n,m}: " + value, column);
                                }
                            } else if (character == ')') {
                                // forcing this to non-capturing as it is really not part of the literal value contained therein
                                partList.add(new RegexPart(")", RegexType.REGEX, nonCapturing));
                                column += 1;
                                String closing = parensAndBrackets.removeLast();
                                if (closing.charAt(0) != '(') {
                                    throw new JavaRegexParseException("Invalid Regular Expression: unexpected closing paren", column);
                                }
                                if (closing.length() > 1) {
                                    nonCapturing--;
                                }
                            } else if (character == ']') {
                                partList.add(new RegexPart("]", RegexType.REGEX, nonCapturing));
                                column += 1;
                                String closing = parensAndBrackets.removeLast();
                                if (closing.charAt(0) != '[') {
                                    throw new JavaRegexParseException("Invalid Regular Expression: unexpected closing square bracket", column);
                                }
                                bracketCount--;
                            } else if (character == '}') {
                                partList.add(new RegexPart("}", RegexType.REGEX, nonCapturing));
                                column += 1;
                                String closing = parensAndBrackets.removeLast();
                                if (closing.charAt(0) != '{') {
                                    throw new JavaRegexParseException("Invalid Regular Expression: unexpected closing curly bracket", column);
                                }
                            } else if (QUANTIFIERS.indexOf(character) >= 0) {
                                partList.add(new RegexPart(Character.toString(character), RegexType.REGEX_QUANTIFIER, nonCapturing));
                                column += 1;
                            } else if (BOUNDARY_CHARS.indexOf(character) >= 0) {
                                partList.add(new RegexPart(Character.toString(character), RegexType.IGNORABLE_REGEX, nonCapturing));
                                column += 1;
                            } else {
                                partList.add(new RegexPart(Character.toString(character), RegexType.REGEX, nonCapturing));
                                column += 1;
                            }
                        } else if (bracketCount > 0 && character == '&' && c < remainder.length() - 1 && remainder.charAt(c + 1) == '&') {
                            // this is a special case in a character class contruct
                            partList.add(new RegexPart("&&", RegexType.REGEX, nonCapturing));
                            column += 2;
                            c++;
                        } else {
                            partList.add(new RegexPart(Character.toString(character), RegexType.LITERAL, nonCapturing));
                            column += 1;
                        }
                    }
                }
                remainder = null;
            }
        }
        if (!parensAndBrackets.isEmpty()) {
            throw new JavaRegexParseException("Invalid Regular Expression: missing closing paren or bracket", column);
        }
        if (quoted) {
            throw new JavaRegexParseException("Invalid Regular Expression: missing closing quoted section (\\E)", column);
        }
        regexParts = partList.toArray(new RegexPart[partList.size()]);
    }
    
    /**
     * Determine the leading and trailing literals, and update the hasWildCard boolean while we are at it
     */
    private void updateLiteral() {
        if (!updatedLiterals) {
            updateLeadingLiteralAndWildCard();
            updateTrailingLiteral();
            updatedLiterals = true;
        }
    }
    
    private void updateLeadingLiteralAndWildCard() {
        leadingLiteral = null;
        hasWildCard = false;
        
        // a stack of literal builders used for nested capturing groups. If empty then we are at the top level.
        LinkedList<StringBuilder> literalBuilders = new LinkedList<>();
        // the current literal builder
        StringBuilder literalBuilder = new StringBuilder();
        // appendLiteral is set false once we have found a regex and we need to terminate with what we have
        boolean appendLiteral = true;
        
        for (int i = 0; i < regexParts.length; i++) {
            RegexPart part = regexParts[i];
            
            // if we are done and we have already resolved all nestings, then we are done
            if (!appendLiteral && literalBuilders.isEmpty()) {
                break;
            }
            
            // simply ignore nonCapturing portions
            if (part.nonCapturing) {
                continue;
            }
            
            // ignore the ignorable
            if (part.type.equals(RegexType.IGNORABLE_REGEX)) {
                continue;
            }
            
            // if a literal then append to the current builder
            if (part.type.isLiteral()) {
                if (appendLiteral && atLeastOnce(i)) {
                    if (part.type == RegexType.ESCAPED_LITERAL) {
                        literalBuilder.append(part.regex.substring(1));
                    } else {
                        literalBuilder.append(part.regex);
                    }
                }
            }
            // if a capturing group, the push the literal builders
            else if (part.regex.equals("(")) {
                literalBuilders.addLast(literalBuilder);
                literalBuilder = new StringBuilder();
            }
            // if ending a capturing group, then pop the literal builders, appending as appropriate
            else if (part.regex.equals(")")) {
                if (atLeastOnce(i)) {
                    literalBuilders.getLast().append(literalBuilder);
                }
                literalBuilder = literalBuilders.removeLast();
            } else {
                // if a logical OR, then empty the literal we are working on
                if (part.regex.equals("|")) {
                    literalBuilder.setLength(0);
                }
                
                // we are now done appending literals as we have found a non-literal
                appendLiteral = false;
                
                // we can set the hasWildCard to true now
                hasWildCard = true;
            }
        }
        if (literalBuilder.length() > 0) {
            leadingLiteral = literalBuilder.toString();
        }
    }
    
    private void updateTrailingLiteral() {
        trailingLiteral = null;
        
        // appendLiteral is set false once we have found a regex and we need to terminate with what we have
        boolean appendLiteral = true;
        
        // a stack of literal builders used for nested capturing groups. If empty then we are at the top level.
        LinkedList<StringBuilder> literalBuilders = new LinkedList<>();
        // the current literal builder
        StringBuilder literalBuilder = new StringBuilder();
        // a stack of atLeastOnce flags which tracks with the literalBuilders stack
        LinkedList<Boolean> atLeastOnceFlags = new LinkedList<>();
        // the current atLeastOnce flag
        boolean atLeastOnce = true;
        
        // have we found a quantifier yet
        boolean quantifierFound = false;
        
        for (int i = regexParts.length - 1; i >= 0; i--) {
            RegexPart part = regexParts[i];
            
            // if we are done and we have already resolved all nestings, then we are done
            if (!appendLiteral && literalBuilders.isEmpty()) {
                break;
            }
            
            // simply ignore nonCapturing portions
            if (part.nonCapturing) {
                continue;
            }
            
            // ignore the ignorable
            if (part.type.equals(RegexType.IGNORABLE_REGEX)) {
                continue;
            }
            
            // ignore quantifiers
            if (part.type == RegexType.REGEX_QUANTIFIER) {
                // if we may have none of the preceding value, then we are done
                if (!atLeastOnce(i - 1)) {
                    appendLiteral = false;
                }
                quantifierFound = true;
                continue;
            }
            
            // if a literal then prepend to the current builder
            if (part.type.isLiteral()) {
                if (appendLiteral) {
                    if (part.type == RegexType.ESCAPED_LITERAL) {
                        literalBuilder.insert(0, part.regex.substring(1));
                    } else {
                        literalBuilder.insert(0, part.regex);
                    }
                    // if a quantifier was found at the top level, then we are now done
                    if (quantifierFound && literalBuilders.isEmpty()) {
                        appendLiteral = false;
                    }
                }
            }
            // if a capturing group, the push the literal builders
            else if (part.regex.equals(")")) {
                literalBuilders.addLast(literalBuilder);
                atLeastOnceFlags.addLast(atLeastOnce);
                literalBuilder = new StringBuilder();
                atLeastOnce = atLeastOnce(i);
            }
            // if ending a capturing group, then pop the literal builders, appending as appropriate
            else if (part.regex.equals("(")) {
                if (atLeastOnce) {
                    literalBuilders.getLast().insert(0, literalBuilder);
                }
                literalBuilder = literalBuilders.removeLast();
                atLeastOnce = atLeastOnceFlags.removeLast();
                
                // if a quantifier was found, then we are done
                if (quantifierFound) {
                    appendLiteral = false;
                }
            }
            // else some other regex
            else {
                // if a logical OR, then empty the literal we are working on
                if (part.regex.equals("|")) {
                    literalBuilder.setLength(0);
                }
                
                // we are now done appending literals as we have found a non-literal
                appendLiteral = false;
            }
        }
        if (literalBuilder.length() > 0) {
            trailingLiteral = literalBuilder.toString();
        }
    }
    
    /**
     * Determine if the part at index i is to occur at least once as determined by an optional following regex quantifier
     * 
     * @param i
     */
    private boolean atLeastOnce(int i) {
        // only use this literal if the following part is not ?, *, or {0,...
        boolean atLeastOnce = true;
        if (followedByQuantifier(i)) {
            if (regexParts[i + 1].regex.equals("?") || regexParts[i + 1].regex.equals("*")) {
                atLeastOnce = false;
            } else if (regexParts[i + 1].regex.equals("{0}") || regexParts[i + 1].regex.startsWith("{0,")) {
                atLeastOnce = false;
            }
        }
        return atLeastOnce;
    }
    
    /**
     * Determine if the part at index i is followed by a quantifier
     * 
     * @param i
     */
    private boolean followedByQuantifier(int i) {
        return (i < (regexParts.length - 1) && regexParts[i + 1].type == RegexType.REGEX_QUANTIFIER);
    }
    
    public boolean hasWildCard() {
        updateLiteral();
        return hasWildCard;
    }
    
    public boolean isLeadingLiteral() {
        updateLiteral();
        return leadingLiteral != null;
    }
    
    public boolean isTrailingLiteral() {
        updateLiteral();
        return trailingLiteral != null;
    }
    
    public boolean isLeadingRegex() {
        updateLiteral();
        return leadingLiteral == null;
    }
    
    public boolean isTrailingRegex() {
        updateLiteral();
        return trailingLiteral == null;
    }
    
    public boolean isNgram() {
        updateLiteral();
        return (leadingLiteral == null && trailingLiteral == null);
    }
    
    public String getLeadingLiteral() {
        updateLiteral();
        return leadingLiteral;
    }
    
    public String getTrailingLiteral() {
        updateLiteral();
        return trailingLiteral;
    }
    
    public String getLeadingOrTrailingLiteral() {
        updateLiteral();
        return (leadingLiteral != null ? leadingLiteral : trailingLiteral);
    }
    
    /**
     * Given an ip regex, zero pad it out to create a regex for the normalized ip value.
     * 
     * This method does not attempt to discern the intent of the user when a wildcard is specified mid-octet. It always tries to zero-pad the octet in which the
     * wildcard was found
     * 
     * For example, 1.2.1* has the potential to match 001.002.001.*, 001.002.010.*, or 001.002.100.*. This method will return an expansion of 001.002.001.* for
     * that input.
     * 
     * @return If the zero-padded variant consists of octets of length 3, the zero-padded regex variant. Else, the original ip address.
     * @throws JavaRegexParseException
     */
    public String getZeroPadIpRegex() throws JavaRegexParseException {
        StringBuilder builder = new StringBuilder();
        
        RegexPart split = new RegexPart("\\.", RegexType.ESCAPED_LITERAL, false);
        
        // split up the parts into those that would match against a tuple
        // to do that we find the literal '.' matches
        List<RegexPart[]> tuples = splitParts(this.regexParts, split);
        
        List<RegexPart> ignore = Arrays.asList(split, ALTERNATE, OPEN_PAREN, CLOSE_PAREN);
        
        // if we found a tuple that crosses over an open group or a close group, then we have a situation
        // we cannot handle currently. This gets even more complicates with alternatives within the groups.
        // (e.g. \\.m(n\\.o)p\\. in which case the tuples are actually mn and op)
        boolean inTuple = false;
        String previousTuple = null;
        
        // now for each tuple, prefix with '0' literals as needed
        for (RegexPart[] tuple : tuples) {
            if (tuple.length != 1 || !ignore.contains(tuple[0])) {
                if (inTuple) {
                    throw new JavaRegexParseException(
                                    "Currently cannot handle tuples that cross over group boundaries: " + previousTuple + " and " + getRegex(tuple), -1);
                }
                inTuple = true;
                previousTuple = getRegex(tuple);
                
                if (!allDigits(tuple)) {
                    throw new JavaRegexParseException("This tuple matches non digits and hence cannot match an IPV4: " + previousTuple, -1);
                }
                
                // now prefix with '0' literals from 3-min to 3-max
                int[] bounds = countMatchedChars(tuple);
                if (bounds[MIN_INDEX] < 3) {
                    // verify that the characters matched would actually be digits
                    if (bounds[MIN_INDEX] == bounds[MAX_INDEX]) {
                        int count = 3 - bounds[MIN_INDEX];
                        for (int i = 0; i < count; i++) {
                            builder.append('0');
                        }
                    } else {
                        int lower = 3 - bounds[MAX_INDEX];
                        if (lower < 0) {
                            lower = 0;
                        }
                        int upper = 3 - bounds[MIN_INDEX];
                        builder.append("0{").append(lower).append(',').append(upper).append('}');
                    }
                }
            } else if (tuple[0].equals(split)) {
                inTuple = false;
            } else if (tuple[0].equals(ALTERNATE)) {
                inTuple = false;
            }
            builder.append(getRegex(tuple));
        }
        return builder.toString();
    }
    
    private boolean allDigits(RegexPart[] tuple) {
        LinkedList<Boolean> negatedCharClass = new LinkedList<>();
        boolean negated = false;
        for (RegexPart part : tuple) {
            switch (part.type) {
                case LITERAL:
                    for (int i = 0; i < part.regex.length(); i++) {
                        if (negated == Character.isDigit(part.regex.charAt(i))) {
                            return false;
                        }
                    }
                    break;
                case ESCAPED_LITERAL:
                    if (negated == Character.isDigit(part.regex.charAt(1))) {
                        return false;
                    }
                    break;
                case REGEX:
                    if (part.regex.equals("[")) {
                        negatedCharClass.addLast(Boolean.valueOf(negated));
                    } else if (part.regex.equals("[^")) {
                        negatedCharClass.addLast(Boolean.valueOf(negated));
                        negated = !negated;
                    } else if (part.regex.equals("]")) {
                        negated = negatedCharClass.removeLast();
                    }
                    break;
                case ESCAPED_REGEX: {
                    if (part.regex.charAt(1) == 'p' || part.regex.charAt(1) == 'P') {
                        if (negated == DIGIT_CHARACTER_CLASSES.contains(part.regex)) {
                            return false;
                        }
                    } else if (negated == (NON_DIGIT_ESCAPED_REGEX_CHARS.indexOf(part.regex.charAt(1)) < 0)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    /**
     * Split up a list of parts using a specified separator. If a separator is found inside of a nested group, then that group and its ancestors begin and end
     * parentheses will be returned as separate parts. Separators are included as separate parts.
     * 
     * @param character
     * @param escaped
     * @return the part lists
     * @throws JavaRegexParseException
     */
    public List<String> splitParts(char character, boolean escaped) throws JavaRegexParseException {
        // create the part
        RegexType type = (escaped ? RegexType.ESCAPED_LITERAL : RegexType.LITERAL);
        if (escaped && (ESCAPED_REGEX_CHARS.indexOf(character) >= 0)) {
            if (QUOTING_REGEX_CHARS.indexOf(character) >= 0 || BOUNDARY_REGEX_CHARS.indexOf(character) >= 0) {
                type = RegexType.IGNORABLE_REGEX;
            } else {
                type = RegexType.ESCAPED_REGEX;
            }
        } else if (!escaped && (RESERVED_CHARS.indexOf(character) >= 0)) {
            if (BOUNDARY_CHARS.indexOf(character) >= 0) {
                type = RegexType.IGNORABLE_REGEX;
            } else {
                type = RegexType.REGEX;
            }
        }
        List<RegexPart[]> parts = splitParts(this.regexParts, new RegexPart(Character.toString(character), type, false));
        List<String> regex = new ArrayList<>();
        for (RegexPart[] part : parts) {
            regex.add(getRegex(part));
        }
        return regex;
    }
    
    /**
     * Split up a list of parts using a specified separator. If a separator is found inside of a nested group, then that group and its ancestors begin and end
     * parentheses will be returned as separate parts as well as alternates. Separators are included as separate parts.
     * 
     * @param parts
     * @param separator
     * @return the part lists
     * @throws JavaRegexParseException
     */
    private List<RegexPart[]> splitParts(RegexPart[] parts, RegexPart separator) throws JavaRegexParseException {
        LinkedList<RegexPart[]> tuples = new LinkedList<>();
        int start = 0;
        int level = 0;
        int separationLevel = 0;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equals(OPEN_PAREN)) {
                level++;
            } else if (parts[i].equals(CLOSE_PAREN)) {
                if (level == 0) {
                    throw new JavaRegexParseException("Non matching groups", -1);
                }
                level--;
                if (level < separationLevel) {
                    if (start < i) {
                        tuples.addLast(Arrays.copyOfRange(parts, start, i));
                    }
                    tuples.addLast(new RegexPart[] {CLOSE_PAREN});
                    start = i + 1;
                    // now move the separation level up one
                    separationLevel = level;
                }
            } else if (parts[i].equals(separator)) {
                // split up appropriately accounting for levels and alternates
                separationLevel = level;
                int begin = start;
                for (int j = start; j < i; j++) {
                    if (parts[j].equals(ALTERNATE)) {
                        if (start < j) {
                            tuples.addLast(Arrays.copyOfRange(parts, start, j));
                        }
                        tuples.addLast(new RegexPart[] {ALTERNATE});
                        start = j + 1;
                    } else if (parts[j].equals(OPEN_PAREN)) {
                        if (start < j) {
                            tuples.addLast(Arrays.copyOfRange(parts, start, j));
                        }
                        tuples.addLast(new RegexPart[] {OPEN_PAREN});
                        start = j + 1;
                    } else if (parts[j].equals(CLOSE_PAREN)) {
                        // we found a closing paren which must have a matching open paren within a non-separated section
                        
                        // move back to the matching paren
                        RegexPart[] last = tuples.removeLast();
                        while (last.length != 1 || !last[0].equals(OPEN_PAREN)) {
                            start -= last.length;
                            last = tuples.removeLast();
                        }
                        start -= last.length;
                        last = (tuples.isEmpty() || start == begin ? null : tuples.removeLast());
                        
                        // now move back to the previous paren, separator, or beginning
                        while (last != null && (last.length != 1 || !(last[0].equals(OPEN_PAREN) || last[0].equals(separator)))) {
                            start -= last.length;
                            last = (tuples.isEmpty() || start == begin ? null : tuples.removeLast());
                        }
                        if (last != null) {
                            tuples.addLast(last);
                        }
                    }
                }
                
                if (i > start) {
                    tuples.addLast(Arrays.copyOfRange(parts, start, i));
                }
                tuples.addLast(new RegexPart[] {separator});
                start = i + 1;
            }
        }
        if (level > 0) {
            throw new JavaRegexParseException("Non matching groups", -1);
        }
        if (parts.length > start) {
            tuples.addLast(Arrays.copyOfRange(parts, start, parts.length));
        }
        
        return tuples;
    }
    
    /**
     * Determine the minimum and maximum number of characters that a regex will match
     * 
     * @return the min and max number of characters that the regex will match
     * @throws JavaRegexParseException
     */
    public int[] countMatchedChars() throws JavaRegexParseException {
        return countMatchedChars(this.regexParts);
    }
    
    /**
     * Determine the minimum and maximum number of characters that a regex will match
     * 
     * @param parts
     * @return the min and max number of characters that the regex will match
     * @throws JavaRegexParseException
     */
    private int[] countMatchedChars(RegexPart[] parts) throws JavaRegexParseException {
        
        // a stack of ranges used for nested capturing groups with alternates. If empty then we are at the top level.
        LinkedList<LinkedList<int[]>> groups = new LinkedList<>();
        
        // the current alternates. If empty then we have no alternates
        LinkedList<int[]> alternates = new LinkedList<>();
        
        // the current bounds
        int[] bounds = new int[2];
        
        // are we in a character class section [...]
        int charClass = 0;
        
        // now count the digits
        int column = 0;
        int len = parts.length;
        for (int partIndex = 0; partIndex < len; partIndex++) {
            RegexPart part = parts[partIndex];
            if (part.nonCapturing) {
                continue;
            }
            if (charClass > 0) {
                if (part.type == RegexType.REGEX) {
                    if (part.regex.equals("]")) {
                        charClass--;
                        if (charClass == 0) {
                            updateBounds(bounds, 1, parts, partIndex);
                        }
                    } else if (part.regex.startsWith("[")) {
                        charClass++;
                    }
                }
            } else {
                switch (part.type) {
                    case LITERAL:
                        updateBounds(bounds, part.regex.length(), parts, partIndex);
                        break;
                    case ESCAPED_LITERAL:
                        updateBounds(bounds, part.regex.length() - 1, parts, partIndex);
                        break;
                    case ESCAPED_REGEX:
                        if (CHAR_REGEX_CHARS.indexOf(part.regex.charAt(1)) >= 0) {
                            updateBounds(bounds, 1, parts, partIndex);
                        } else if (BACK_REF_CHARS.indexOf(part.regex.charAt(1)) >= 0) {
                            // unsupported
                            throw new JavaRegexParseException("Cannot deal with back references in zeroPadRegex", column);
                        }
                        break;
                    case REGEX:
                        if (part.regex.equals("(")) {
                            alternates.addLast(bounds);
                            groups.addLast(alternates);
                            bounds = new int[2];
                            alternates = new LinkedList<>();
                        } else if (part.regex.equals(")")) {
                            if (alternates.isEmpty()) {
                                throw new JavaRegexParseException("Found an illegal close ')' to a group without the open '('", column);
                            }
                            alternates.addLast(bounds);
                            int[] update = summarize(alternates);
                            alternates = groups.removeLast();
                            bounds = alternates.removeLast();
                            updateBounds(bounds, update, parts, partIndex);
                        } else if (part.regex.startsWith("[")) {
                            charClass++;
                        } else if (part.regex.equals("]")) {
                            // this would have been handled above...
                            throw new JavaRegexParseException("Found an illegal close ']' to a character class without the open '['", column);
                        } else if (part.regex.equals("|")) {
                            alternates.addLast(bounds);
                            bounds = new int[2];
                        } else if (part.regex.equals(".")) {
                            updateBounds(bounds, 1, parts, partIndex);
                        } else {
                            // unsupported
                            throw new JavaRegexParseException("Cannot deal with " + part.regex + " in zeroPadRegex", column);
                        }
                        break;
                    case REGEX_QUANTIFIER:
                        // already handled in updateBounds....skip
                        break;
                    case IGNORABLE_REGEX:
                        // ignore the ignorable
                        break;
                }
            }
        }
        alternates.addLast(bounds);
        return summarize(alternates);
    }
    
    /**
     * Summarize a list of alternatives returning the min of the mins and the max of the maxes
     * 
     * @param alternates
     * @return the summarized bounds
     */
    private static int[] summarize(LinkedList<int[]> alternates) {
        int[] bounds = alternates.removeLast();
        while (!alternates.isEmpty()) {
            int[] alternate = alternates.removeLast();
            bounds[MIN_INDEX] = Math.min(bounds[MIN_INDEX], alternate[MIN_INDEX]);
            bounds[MAX_INDEX] = Math.max(bounds[MAX_INDEX], alternate[MAX_INDEX]);
        }
        return bounds;
    }
    
    /**
     * Updated the bounds with a quantity, taking a following quantifier into account.
     * 
     * @param bounds
     *            The bounds to update
     * @param quantity
     *            The quantity to update both the min and max with
     * @param parts
     *            The regex parts
     * @param partIndex
     *            The current regex pointer
     * @throws JavaRegexParseException
     */
    private void updateBounds(int[] bounds, int quantity, RegexPart[] parts, int partIndex) throws JavaRegexParseException {
        updateBounds(bounds, new int[] {quantity, quantity}, parts, partIndex);
    }
    
    /**
     * Updated the bounds with a min and max quantity, taking a following quantifier into account.
     * 
     * @param bounds
     *            The bounds to update
     * @param quantity
     *            The min and max quantity to update with
     * @param parts
     *            The regex parts
     * @param partIndex
     *            The current regex pointer
     * @throws JavaRegexParseException
     */
    private void updateBounds(int[] bounds, int[] quantity, RegexPart[] parts, int partIndex) throws JavaRegexParseException {
        int nextIndex = partIndex + 1;
        int[] multiplier = new int[] {1, 1};
        if (nextIndex < parts.length && parts[nextIndex].type == RegexType.REGEX_QUANTIFIER) {
            RegexPart part = parts[nextIndex];
            // up by the max count
            Matcher matcher = curlyQuantifierPattern.matcher(part.regex);
            if (matcher.matches()) {
                if (matcher.groupCount() == 3 && !matcher.group(3).isEmpty()) {
                    multiplier[MAX_INDEX] = Integer.parseInt(matcher.group(3));
                } else {
                    multiplier[MAX_INDEX] = Integer.parseInt(matcher.group(1));
                }
                multiplier[MIN_INDEX] = Integer.parseInt(matcher.group(1));
            } else if (part.regex.equals("?")) {
                multiplier[MIN_INDEX] = 0;
                multiplier[MAX_INDEX] = 1;
            } else if (part.regex.equals("+")) {
                multiplier[MIN_INDEX] = 1;
                multiplier[MAX_INDEX] = Integer.MAX_VALUE / 2;
            } else if (part.regex.equals("*")) {
                multiplier[MIN_INDEX] = 0;
                multiplier[MAX_INDEX] = Integer.MAX_VALUE / 2;
            } else {
                throw new JavaRegexParseException("Cannot deal with the quantifier " + part.regex, -1);
            }
        }
        
        bounds[MIN_INDEX] += quantity[MIN_INDEX] * multiplier[MIN_INDEX];
        bounds[MAX_INDEX] += quantity[MAX_INDEX] * multiplier[MAX_INDEX];
    }
    
    /**
     * Apply uppercase or lowercase to a regex, leaving all character class constants alone. It will replace \\p{Lu}, \\p{Lower}, \\p{Upper} \\p{javaLowerCase}
     * and \\p{javaUpperCase} as well. TODO: Nested negations of upper or lower character classes are not be handled correctly.
     * 
     * @param upperCase
     */
    public void applyRegexCaseSensitivity(boolean upperCase) {
        // one possibility is to simply add the case independence flag...but does not
        // work for shardIndex query....maybe we can modify that logic appropriately....
        // return "(?i" + regex + ')';
        
        // translate the uppercase and lowercase character classes
        // and apply the upcase or lowercase to all literals
        for (int i = 0; i < regexParts.length; i++) {
            RegexPart part = regexParts[i];
            if (part.type.isLiteral()) {
                part.regex = (upperCase ? part.regex.toUpperCase() : part.regex.toLowerCase());
            } else {
                // check for \\p{Lower} or \\p{Upper}
                // check for \\p{javaLowerCase} or \\p{javaUpperCase}
                // check for \\p{Lu}
                boolean negated = (i > 0 && regexParts[i - 1].regex.equals("[^"));
                if ((upperCase != negated) && part.regex.equals("\\p{Lower}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{Upper}";
                    }
                } else if ((upperCase != negated) && part.regex.equals("\\p{javaLowerCase}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{javaUpperCase}";
                    }
                } else if ((upperCase == negated) && part.regex.equals("\\p{Upper}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{Lower}";
                    }
                } else if ((upperCase == negated) && part.regex.equals("\\p{javaUpperCase}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{javaLowerCase}";
                    }
                } else if ((upperCase == negated) && part.regex.equals("\\p{Lu}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{L}";
                    }
                } else if ((upperCase != negated) && part.regex.equals("\\P{Upper}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{Upper}";
                    }
                } else if ((upperCase != negated) && part.regex.equals("\\P{javaUpperCase}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{javaUpperCase}";
                    }
                } else if ((upperCase == negated) && part.regex.equals("\\P{Lower}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{Lower}";
                    }
                } else if ((upperCase == negated) && part.regex.equals("\\P{javaLowerCase}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{javaLowerCase}";
                    }
                } else if ((upperCase != negated) && part.regex.equals("\\P{Lu}")) {
                    if (negated) {
                        regexParts[i - 1].regex = "[";
                    } else {
                        part.regex = "\\p{L}";
                    }
                }
                
            }
        }
        
        // now reset the updated value
        updatedLiterals = false;
    }
    
}
