package datawave.query.language.functions;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public interface QueryFunction {

    void validate() throws IllegalArgumentException;

    void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException;

    String getName();

    void setName(String name);

    List<String> getParameterList();

    void setParameterList(List<String> parameterList);

    QueryFunction duplicate();
}
