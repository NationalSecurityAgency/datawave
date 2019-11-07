package datawave.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import datawave.query.Constants;

public class JexlSelectorNode extends JexlNode {
    public enum Type {
        EXACT, WILDCARDS, REGEX
    }
    
    private static final Character BACKSLASH = '\\';
    private static final String UNICODE_BACKSLASH = "\\u005c";
    
    private static final Set<Character> ESCAPE_CHARS = ImmutableSet.<Character> builder().add('(').add('[').add('\\').add('^').add('$').add('.').add('|')
                    .add(')').add(']').add('}').add('?').add('*').add('+').build();
    
    private Type type = null;
    private String field = null;
    private String selector = null;
    
    private JexlSelectorNode() {
        super(new ArrayList<>());
    }
    
    public JexlSelectorNode(Type type, String field, String selector) {
        super(new ArrayList<>());
        this.type = type;
        this.field = field;
        this.selector = selector;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (field != null && !field.isEmpty()) {
            sb.append(field);
        } else {
            // Apply a special field name for "unfielded" queries
            sb.append(Constants.ANY_FIELD);
        }
        
        switch (type) {
            case EXACT:
                sb.append(" == ");
                
                sb.append("'");
                sb.append(jexlEscapeSelector(selector));
                sb.append("'");
                break;
            case WILDCARDS:
                // more work needs to be done here to escape sequences that
                // will be interpreted by the Jexl regex evaluator
                // \\ \* and \? should already be handled
                
                sb.append(" =~ ");
                
                String regex = selector;
                regex = convertToRegex(regex);
                
                sb.append("'");
                sb.append(regex);
                sb.append("'");
                break;
            case REGEX:
                // more work needs to be done here to escape sequences that
                // will be interpreted by the Jexl regex evaluator
                // \\ \* and \? should already be handled
                
                sb.append(" =~ ");
                
                sb.append("'");
                sb.append(selector);
                sb.append("'");
                break;
        }
        
        return sb.toString();
    }
    
    public static String convertToRegex(String s) {
        StringBuilder sb = new StringBuilder();
        char[] chars = s.toCharArray();
        
        int lastIndex = chars.length - 1;
        for (int x = 0; x < chars.length; x++) {
            char currChar = chars[x];
            
            if (currChar == BACKSLASH) {
                if (x < lastIndex) {
                    x++;
                    
                    // For these chars, we need to escape them to do what the user wants
                    if (ESCAPE_CHARS.contains(chars[x])) {
                        sb.append(UNICODE_BACKSLASH);
                    }
                    
                    if (chars[x] == BACKSLASH) {
                        sb.append(UNICODE_BACKSLASH);
                    } else {
                        sb.append(chars[x]);
                    }
                }
            } else if (currChar == '*') {
                sb.append(".*?");
            } else if (currChar == '?') {
                sb.append(".");
            } else if (currChar == '.') {
                sb.append(UNICODE_BACKSLASH).append(currChar);
            } else if (currChar == '\'' && (x == 0 || (x > 0 && chars[x - 1] != BACKSLASH))) {
                sb.append(BACKSLASH).append(currChar);
            } else {
                sb.append(currChar);
            }
        }
        
        // Check for a trailing backslash that might escape the ending quotation mark in Jexl
        if (BACKSLASH.equals(sb.charAt(sb.length() - 1))) {
            if (sb.length() > 1) {
                // If we have more than two chars, we need to make sure we don't inadvertently undo the correct escape
                if (!BACKSLASH.equals(sb.charAt(sb.length() - 2))) {
                    sb.append(UNICODE_BACKSLASH);
                }
            } else {
                // Is only a backslash, and as such we need to escape it
                sb.append(UNICODE_BACKSLASH);
            }
        }
        
        return sb.toString();
    }
    
    public String getField() {
        return field;
    }
    
    public String getSelector() {
        return selector;
    }
}
