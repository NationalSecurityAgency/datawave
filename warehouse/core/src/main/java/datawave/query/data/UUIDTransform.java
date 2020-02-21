package datawave.query.data;

import java.util.regex.Pattern;

/**
 * Transform UUID rule, defines a pattern and replacement to be applied to a UUID value
 */
public class UUIDTransform {
    private String regex;
    private String replacement;
    private Pattern pattern;
    
    public UUIDTransform() {
        
    }
    
    public UUIDTransform(String regex, String replacement) {
        this.regex = regex;
        this.replacement = replacement;
        pattern = Pattern.compile(regex);
    }
    
    public String getRegex() {
        return regex;
    }
    
    public void setRegex(String regex) {
        this.regex = regex;
        pattern = Pattern.compile(regex);
    }
    
    public String getReplacement() {
        return replacement;
    }
    
    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }
    
    public Pattern getPattern() {
        return pattern;
    }
}
