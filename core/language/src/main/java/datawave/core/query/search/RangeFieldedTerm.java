package datawave.core.query.search;

public class RangeFieldedTerm extends FieldedTerm {
    private String beginRange = null;
    private String endRange = null;
    private Boolean lowerInclusive = null;
    private Boolean upperInclusive = null;

    public RangeFieldedTerm() {
        this.field = "";
        this.beginRange = "";
        this.endRange = "";
        this.lowerInclusive = true;
        this.upperInclusive = true;
        this.query = null;
    }

    public RangeFieldedTerm(String field, String beginRange, String endRange, Boolean lowerInclusive, Boolean upperInclusive) {
        this.field = field;
        this.beginRange = unescapeSelector(beginRange);
        this.endRange = unescapeSelector(endRange);
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.query = generateQuery();
    }

    @Override
    public String getRangeBegin(EscapedCharacterTreatment escapedCharacterTreatment) {
        String returnString = null;

        if (escapedCharacterTreatment == EscapedCharacterTreatment.ESCAPED) {
            returnString = beginRange;
        } else {
            returnString = unescapeSelector(beginRange);
        }

        if (lowerInclusive == false) {
            returnString = incrementOneCodepoint(returnString);
        }

        return returnString;
    }

    @Override
    public String getRangeEnd(EscapedCharacterTreatment escapedCharacterTreatment) {
        String returnString = null;

        if (escapedCharacterTreatment == EscapedCharacterTreatment.ESCAPED) {
            returnString = endRange;
        } else {
            returnString = unescapeSelector(endRange);
        }

        if (upperInclusive == false) {
            returnString = decrementOneCodepoint(returnString);
        }

        return returnString;
    }

    public Boolean getLowerInclusive() {
        return lowerInclusive;
    }

    public void setLowerInclusive(Boolean lowerInclusive) {
        this.lowerInclusive = lowerInclusive;
    }

    public Boolean getUpperInclusive() {
        return upperInclusive;
    }

    public void setUpperInclusive(Boolean upperInclusive) {
        this.upperInclusive = upperInclusive;
    }

    @Override
    public boolean isMatch(String compareTerm) {
        String field = parseField(compareTerm);
        String selector = parseSelector(compareTerm);
        return isFieldMatch(field) && isSelectorMatch(selector);
    }

    private String generateQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append(field);
        sb.append(":");
        String beginBrace = (lowerInclusive == true) ? "[" : "{";
        String endBrace = (upperInclusive == true) ? "]" : "}";
        sb.append(beginBrace);
        sb.append(beginRange);
        sb.append(" TO ");
        sb.append(endRange);
        sb.append(endBrace);
        return sb.toString();
    }

    @Override
    public String toString() {
        return query;
    }

    /*
     * Only use this call for lexicographic comparison.
     */
    @Override
    public boolean isSelectorMatch(String selector) {
        int beginComp = selector.compareToIgnoreCase(beginRange);
        int endComp = selector.compareToIgnoreCase(endRange);

        Boolean withinLower = null;
        Boolean withinUpper = null;

        if (lowerInclusive) {
            withinLower = beginComp >= 0;
        } else {
            withinLower = beginComp > 0;
        }

        if (upperInclusive) {
            withinUpper = endComp <= 0;
        } else {
            withinUpper = endComp < 0;
        }
        return withinLower && withinUpper;
    }

    /*
     * No single selector, uses beginRange, endRange
     */
    @Override
    public String getSelector() {
        StringBuilder sb = new StringBuilder();
        String beginBrace = (lowerInclusive == true) ? "[" : "{";
        String endBrace = (upperInclusive == true) ? "]" : "}";
        sb.append(beginBrace);
        sb.append(beginRange);
        sb.append(" TO ");
        sb.append(endRange);
        sb.append(endBrace);
        return sb.toString();
    }

    /*
     * No single selector, uses beginRange, endRange
     */
    @Override
    public void setSelector(String selector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(FieldedTerm o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RangeFieldedTerm))
            return false;
        RangeFieldedTerm other = (RangeFieldedTerm) o;
        return this.toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
