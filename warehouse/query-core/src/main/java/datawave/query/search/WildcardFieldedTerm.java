package datawave.query.search;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;

/**
 * Keeps track of the query term, location to be searched, and also deals with wildcards.
 */
public class WildcardFieldedTerm extends FieldedTerm {
    private static Logger log = Logger.getLogger(WildcardFieldedTerm.class.getName());
    private Pattern selectorRegex = null;
    private String originalSelector = null;

    public WildcardFieldedTerm(String term) {
        this.field = parseField(term);
        this.selector = parseSelector(term);
        this.selectorRegex = convertToRegex(this.selector.toLowerCase());
        this.originalSelector = this.selector;
        if (this.field == null || this.field.isEmpty()) {
            this.query = this.selector;
        } else {
            this.query = this.field + ":" + this.selector;
        }
    }

    public WildcardFieldedTerm(String field, String selector) {
        super(field, selector);

        this.selectorRegex = convertToRegex(this.selector.toLowerCase());
        this.originalSelector = this.selector;
        if (this.field == null || this.field.isEmpty()) {
            this.query = this.selector;
        } else {
            this.query = this.field + ":" + this.selector;
        }
    }

    public WildcardFieldedTerm(String field, String selector, String regex, int flags) {
        super(field, selector);

        this.selectorRegex = Pattern.compile(regex.toLowerCase(), flags);
        this.originalSelector = this.selector;
        if (this.field == null || this.field.isEmpty()) {
            this.query = this.selector;
        } else {
            this.query = this.field + ":" + this.selector;
        }
    }

    public static Pattern convertToRegex(String s) {
        StringBuilder sb = new StringBuilder();
        char[] chars = s.toCharArray();

        int lastIndex = chars.length - 1;
        for (int x = 0; x < chars.length; x++) {
            char currChar = chars[x];

            if (currChar == '\\' && x < lastIndex && (chars[x + 1] == '*' || chars[x + 1] == '?' || chars[x + 1] == '\\')) {
                x++;
                sb.append("\\");
                sb.append(chars[x]);
            } else if (currChar == '*') {
                sb.append(".*?");
            } else if (currChar == '?') {
                sb.append(".");
            } else {
                sb.append(currChar);
            }
        }

        int flags = Pattern.DOTALL;
        return Pattern.compile(sb.toString(), flags);
    }

    public static boolean hasUnescapedWildcard(FieldQueryNode node, Set<Character> charactersToNotEscape) {
        CharSequence textSeq = node.getText();

        if (textSeq instanceof UnescapedCharSequence) {
            UnescapedCharSequence escSeq = (UnescapedCharSequence) textSeq;

            StringBuilder escBuilder = new StringBuilder(textSeq.length());
            for (int i = 0; i < escSeq.length(); i++) {
                if (escSeq.wasEscaped(i) && !charactersToNotEscape.contains(escSeq.charAt(i))) {
                    escBuilder.append("\\");
                }

                escBuilder.append(escSeq.charAt(i));
            }

            return getFirstWildcardIndex(escBuilder.toString()) > -1;
        } else {
            return getFirstWildcardIndex(textSeq.toString()) > -1;
        }
    }

    public static int getFirstWildcardIndex(String selector) {
        int firstNonEscapedWildcard = -1;

        char[] chars = selector.toCharArray();
        for (int x = 0; x < chars.length; x++) {
            char currChar = chars[x];
            if (currChar == '\\') {
                // skip next character since it's escaped
                x++;
            } else if (currChar == '*' || currChar == '?') {
                firstNonEscapedWildcard = x;
                break;
            }
        }
        return firstNonEscapedWildcard;
    }

    @Override
    public String getRangeBegin(EscapedCharacterTreatment escapedCharacterTreatment) {
        int firstWildcardIndex = getFirstWildcardIndex(this.originalSelector);
        String beginRange = this.originalSelector;
        if (firstWildcardIndex != -1) {
            beginRange = this.originalSelector.substring(0, firstWildcardIndex);
        }

        if (escapedCharacterTreatment == EscapedCharacterTreatment.UNESCAPED) {
            beginRange = unescapeSelector(beginRange);
        }

        return beginRange;
    }

    @Override
    public String getRangeEnd(EscapedCharacterTreatment escapedCharacterTreatment) {
        return incrementOneCodepoint(getRangeBegin(escapedCharacterTreatment));
    }

    @Override
    public boolean isMatch(String compareTerm) {
        String field = parseField(compareTerm);
        String selector = parseSelector(compareTerm);

        return (isFieldMatch(field) && isSelectorMatch(selector));
    }

    @Override
    public String toString() {
        return query;
    }

    public static String parseField(String termString) {
        // check to see if the user specified a section of the data
        int firstQuote = termString.indexOf("\"");
        String termNoQuotes;
        if (firstQuote != -1) {
            termNoQuotes = termString.substring(0, firstQuote);
        } else {
            termNoQuotes = termString;
        }

        int firstColonIndex = termNoQuotes.indexOf(":");
        if (firstColonIndex != -1) {
            return termNoQuotes.substring(0, firstColonIndex);
        }
        return "";
    }

    public static String parseSelector(String termString) {
        // check to see if the user specified a section of the data
        int firstQuote = termString.indexOf("\"");
        String termNoQuotes;
        if (firstQuote != -1) {
            termNoQuotes = termString.substring(0, firstQuote);
        } else {
            termNoQuotes = termString;
        }

        int firstColonIndex = termNoQuotes.indexOf(":");
        if (firstColonIndex == -1) {
            return termNoQuotes;
        } else {
            return termNoQuotes.substring(firstColonIndex + 1);
        }
    }

    /**
     * Determines if the query term matches the row term. The String passed to this method should not contain any field or realm information, only the term.
     *
     * @param selector
     *            a selector
     * @return True if the query term matches this row
     */
    public boolean isSelectorMatch(String selector) {
        Matcher m = this.selectorRegex.matcher(selector.toLowerCase());
        return m.matches();
    }

    /**
     * Determines if this row field (or realm) matches the query term. If no field was specified in the query then this method returns true.
     *
     * @param field
     *            a row field
     * @return Returns False if field is null, otherwise returns true if the query field matches the row field.
     */
    public boolean isFieldMatch(String field) {
        if (field == null) {
            return false;
        } else if (this.field.isEmpty() || field.isEmpty()) {
            return true;
        } else {
            return this.field.equalsIgnoreCase(field);
        }
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setSelector(String selector) {
        this.selectorRegex = convertToRegex(selector.toLowerCase());
        this.originalSelector = selector;
        this.selector = unescapeSelector(selector);
    }

    public String getSelectorRegex() {
        return this.selectorRegex.pattern();
    }
}
