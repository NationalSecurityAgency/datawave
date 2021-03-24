package datawave.query.language.parser.jexl;

import java.util.ArrayList;

public class JexlRangeNode extends JexlNode {
    private String field = null;
    private String beginRange = null;
    private String endRange = null;
    private Boolean lowerInclusive = null;
    private Boolean upperInclusive = null;
    
    private JexlRangeNode() {
        super(new ArrayList<>());
        this.field = "";
        this.beginRange = "";
        this.endRange = "";
        this.lowerInclusive = true;
        this.upperInclusive = true;
    }
    
    public JexlRangeNode(String field, String beginRange, String endRange, Boolean lowerInclusive, Boolean upperInclusive) {
        super(new ArrayList<>());
        this.field = field;
        this.beginRange = jexlEscapeSelector(beginRange);
        this.endRange = jexlEscapeSelector(endRange);
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        String lowerRangeOp = lowerInclusive == true ? ">=" : ">";
        String upperRangeOp = upperInclusive == true ? "<=" : "<";
        
        boolean lowerWildcard = beginRange.equals("*");
        boolean upperWildcard = endRange.equals("*");
        
        boolean isBounded = !lowerWildcard && !upperWildcard;
        
        if (isBounded) {
            sb.append("((_Bounded_ = true) && ");
        }
        
        sb.append("(");
        if (!lowerWildcard) {
            sb.append(field);
            sb.append(" ");
            sb.append(lowerRangeOp);
            sb.append(" '");
            sb.append(beginRange);
            sb.append("'");
        }
        
        if (!lowerWildcard && !upperWildcard) {
            sb.append(" && ");
        }
        
        if (!upperWildcard) {
            sb.append(field);
            sb.append(" ");
            sb.append(upperRangeOp);
            sb.append(" '");
            sb.append(endRange);
            sb.append("'");
        }
        sb.append(")");
        if (isBounded) {
            sb.append(")");
        }
        return sb.toString();
    }
    
    public static String convertToRegex(String s) {
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
            } else if (currChar == '.') {
                sb.append("\\.");
            } else {
                sb.append(currChar);
            }
        }
        return sb.toString();
    }
}
