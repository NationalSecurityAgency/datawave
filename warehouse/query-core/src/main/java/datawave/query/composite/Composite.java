package datawave.query.composite;

import com.google.common.collect.Lists;
import datawave.data.type.DiscreteIndexType;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.List;
import java.util.Map;

public abstract class Composite {
    
    protected final String compositeName; // FU_BA_BAZ
    protected final String separator;
    
    protected final List<String> fieldNameList = Lists.newArrayList(); // [ FOO, BAR, BAZ ]
    protected final List<JexlNode> jexlNodeList = Lists.newArrayList(); // [ JexlEQ, JexlEq, JexlER ]
    protected final List<String> expressionList = Lists.newArrayList(); // [ 'moe', 'larry', 'cu*' ]
    
    public Composite(String compositeName, String separator) {
        this.compositeName = compositeName;
        this.separator = separator;
    }
    
    abstract public Composite clone();
    
    abstract public void addComponent(JexlNode node);
    
    abstract public void getNodesAndExpressions(List<Class<? extends JexlNode>> nodeClasses, List<String> expressions,
                    Map<String,DiscreteIndexType<?>> discreteIndexFieldMap, boolean includeOldData);
    
    abstract public boolean isValid();
    
    abstract public boolean contains(JexlNode node);
    
    public String getCompositeName() {
        return compositeName;
    }
    
    public String getSeparator() {
        return separator;
    }
    
    public List<String> getFieldNameList() {
        return fieldNameList;
    }
    
    public List<JexlNode> getJexlNodeList() {
        return jexlNodeList;
    }
    
    public List<String> getExpressionList() {
        return expressionList;
    }
}
