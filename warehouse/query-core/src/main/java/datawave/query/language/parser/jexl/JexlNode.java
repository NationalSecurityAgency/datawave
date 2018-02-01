package datawave.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

public class JexlNode {
    private List<JexlNode> children = new ArrayList<>();
    
    @SuppressWarnings("unused")
    private JexlNode() {
        
    }
    
    public JexlNode(List<JexlNode> children) {
        this.children = children;
    }
    
    public List<JexlNode> getChildren() {
        return children;
    }
    
    public void setChildren(List<JexlNode> children) {
        this.children = children;
    }
    
    public static String jexlEscapeSelector(String s) {
        String escapedSelector = s;
        escapedSelector = escapedSelector.replace("\\", "\\u005c");
        
        // escape apostrophes becasue we uses these to enclose string
        // literals in jexl syntax.
        escapedSelector = escapedSelector.replace("'", "\\'");
        return escapedSelector;
    }
}
