package datawave.core.query.search;

public abstract class Term {
    private boolean filterOnly = false;

    public enum EscapedCharacterTreatment {
        ESCAPED, UNESCAPED
    }

    public abstract String getRangeBegin(EscapedCharacterTreatment escapedCharacterTreatment);

    public abstract String getRangeEnd(EscapedCharacterTreatment escapedCharacterTreatment);

    public abstract boolean isMatch(String selector);

    public static String unescapeSelector(String s) {
        String escapedSelector = s;
        escapedSelector = escapedSelector.replace("\\\\", "\\");
        escapedSelector = escapedSelector.replace("\\*", "*");
        escapedSelector = escapedSelector.replace("\\?", "?");
        return escapedSelector;
    }

    public static String incrementOneCodepoint(String term) {
        int codepoint = term.codePointBefore(term.length());
        int cpc = term.codePointCount(0, term.length());
        int offset = term.offsetByCodePoints(0, cpc - 1);
        codepoint = codepoint < Integer.MAX_VALUE ? codepoint + 1 : codepoint;
        int cparray[] = {codepoint};
        return term.substring(0, offset) + new String(cparray, 0, 1);
    }

    public static String decrementOneCodepoint(String term) {
        int codepoint = term.codePointBefore(term.length());
        int cpc = term.codePointCount(0, term.length());
        int offset = term.offsetByCodePoints(0, cpc - 1);
        codepoint = codepoint > 0 ? codepoint - 1 : codepoint;
        int cparray[] = {codepoint};
        return term.substring(0, offset) + new String(cparray, 0, 1);
    }

    public boolean isFilterOnly() {
        return filterOnly;
    }

    public void setFilterOnly(boolean filterOnly) {
        this.filterOnly = filterOnly;
    }
}
