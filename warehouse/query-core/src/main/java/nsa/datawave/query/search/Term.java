package nsa.datawave.query.search;

abstract public class Term {
    private boolean filterOnly = false;
    
    public enum EscapedCharacterTreatment {
        ESCAPED, UNESCAPED
    }
    
    abstract public String getRangeBegin(EscapedCharacterTreatment escapedCharacterTreatment);
    
    abstract public String getRangeEnd(EscapedCharacterTreatment escapedCharacterTreatment);
    
    abstract public boolean isMatch(String selector);
    
    static public String unescapeSelector(String s) {
        String escapedSelector = s;
        escapedSelector = escapedSelector.replace("\\\\", "\\");
        escapedSelector = escapedSelector.replace("\\*", "*");
        escapedSelector = escapedSelector.replace("\\?", "?");
        return escapedSelector;
    }
    
    static public String incrementOneCodepoint(String term) {
        int codepoint = term.codePointBefore(term.length());
        int cpc = term.codePointCount(0, term.length());
        int offset = term.offsetByCodePoints(0, cpc - 1);
        codepoint = codepoint < Integer.MAX_VALUE ? codepoint + 1 : codepoint;
        int cparray[] = {codepoint};
        return term.substring(0, offset) + new String(cparray, 0, 1);
    }
    
    static public String decrementOneCodepoint(String term) {
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
