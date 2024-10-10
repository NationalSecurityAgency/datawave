package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.List;

public class NotBooleanQueryNode extends BooleanQueryNode {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public NotBooleanQueryNode(List<QueryNode> clauses) {
        super(clauses);
    }

}
