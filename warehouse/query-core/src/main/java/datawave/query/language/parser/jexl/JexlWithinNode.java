package datawave.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

public class JexlWithinNode extends JexlNode {
    private String field;
    private List<String> wordList;
    private Integer distance;
    
    public JexlWithinNode(String field, List<String> wordList, Integer distance) {
        super(new ArrayList<>());
        this.field = field;
        this.wordList = wordList;
        this.distance = distance;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (this.field == null || this.field.isEmpty()) {
            sb.append("content:within(");
            sb.append(this.distance);
            sb.append(", termOffsetMap");
        } else {
            sb.append("content:within(");
            sb.append(this.field);
            sb.append(", ");
            sb.append(this.distance);
            sb.append(", termOffsetMap");
        }
        
        for (String s : this.wordList) {
            sb.append(", ");
            sb.append("'");
            sb.append(jexlEscapeSelector(s));
            sb.append("'");
        }
        // Matches function argument list
        sb.append(")");
        
        return sb.toString();
    }
    
    public String getField() {
        return this.field;
    }
    
    public List<String> getWordList() {
        return this.wordList;
    }
}
