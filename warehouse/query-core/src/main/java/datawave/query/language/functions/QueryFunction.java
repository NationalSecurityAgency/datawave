package datawave.query.language.functions;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public interface QueryFunction {
    
    public void validate() throws IllegalArgumentException;
    
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException;
    
    public String getName();
    
    public void setName(String name);
    
    public List<String> getParameterList();
    
    public void setParameterList(List<String> parameterList);
    
    public QueryFunction duplicate();
}
