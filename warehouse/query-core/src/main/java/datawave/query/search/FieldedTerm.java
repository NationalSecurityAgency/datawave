package datawave.query.search;

import org.apache.log4j.Logger;

/**
 * Keeps track of the query term, location to be searched, and also deals with wildcards.
 */
public class FieldedTerm extends Term implements Comparable<FieldedTerm> {
    
    protected String field;
    protected String selector;
    protected String unescapedSelector;
    protected String query;
    
    static Logger log = Logger.getLogger(FieldedTerm.class.getName());
    
    public FieldedTerm() {
        this.query = "";
        this.field = "";
        this.selector = "";
        this.unescapedSelector = "";
    }
    
    public FieldedTerm(String term) {
        this.field = parseField(term);
        this.selector = parseSelector(term);
        this.unescapedSelector = unescapeSelector(this.selector);
        if (this.field == null || this.field.isEmpty()) {
            this.query = this.selector;
        } else {
            this.query = this.field + ":" + this.selector;
        }
    }
    
    public FieldedTerm(String field, String selector) {
        this.field = field;
        this.selector = selector;
        this.unescapedSelector = unescapeSelector(this.selector);
        if (field == null || field.isEmpty()) {
            this.query = this.selector;
        } else if (selector == null) {
            throw new IllegalArgumentException("Selector can not be null");
        } else {
            this.query = this.field + ":" + this.selector;
        }
    }
    
    @Override
    public String getRangeBegin(EscapedCharacterTreatment escapedCharacterTreatment) {
        if (escapedCharacterTreatment == EscapedCharacterTreatment.ESCAPED) {
            return selector;
        } else {
            return unescapedSelector;
        }
    }
    
    @Override
    public String getRangeEnd(EscapedCharacterTreatment escapedCharacterTreatment) {
        if (escapedCharacterTreatment == EscapedCharacterTreatment.ESCAPED) {
            return incrementOneCodepoint(selector);
        } else {
            return incrementOneCodepoint(unescapedSelector);
        }
    }
    
    @Override
    public boolean isMatch(String compareTerm) {
        String field = parseField(compareTerm);
        String selector = parseSelector(compareTerm);
        
        return (isFieldMatch(field) && isSelectorMatch(selector));
    }
    
    public boolean isMatch(String field, String selector) {
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
        if (selector == null) {
            return false;
        } else {
            return this.unescapedSelector.equalsIgnoreCase(selector);
        }
    }
    
    /**
     * Determines if this row field (or realm) matches the query term. If no field was specified in the query then this method returns true.
     * 
     * @param field
     *            a field
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
    
    public String getField() {
        return this.field;
    }
    
    public void setField(String field) {
        this.field = field;
    }
    
    public String getSelector() {
        return this.selector;
    }
    
    public void setSelector(String selector) {
        this.selector = selector;
        this.unescapedSelector = unescapeSelector(selector);
    }
    
    @Override
    public int compareTo(FieldedTerm o) {
        return this.toString().compareTo(o.toString());
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FieldedTerm))
            return false;
        FieldedTerm other = (FieldedTerm) o;
        return this.toString().equals(other.toString());
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
