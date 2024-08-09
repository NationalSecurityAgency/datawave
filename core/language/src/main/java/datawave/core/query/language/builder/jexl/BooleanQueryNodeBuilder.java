package datawave.core.query.language.builder.jexl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import datawave.core.query.language.parser.jexl.JexlBooleanNode;
import datawave.core.query.language.parser.jexl.JexlNode;

/**
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object. Every children in the {@link BooleanQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object. <br>
 * <br>
 * It takes in consideration if the children is a {@link ModifierQueryNode} to define the {@link BooleanClause}.
 */
public class BooleanQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;

        JexlNode bNode = null;
        List<QueryNode> children = booleanNode.getChildren();

        if (children != null) {
            List<JexlNode> childrenList = new ArrayList<>();

            LinkedList<QueryNode> extraNodeList = new LinkedList<>();
            boolean isNegation = false;

            for (QueryNode child : children) {
                Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
                if (obj != null) {
                    JexlNode query = (JexlNode) obj;
                    childrenList.add(query);
                }
            }

            bNode = createNode(queryNode, childrenList, isNegation, extraNodeList);
        }

        return bNode;

    }

    private JexlNode createNode(QueryNode queryNode, List<JexlNode> children, boolean isNegation, LinkedList<QueryNode> extraNodeList)
                    throws QueryNodeException {
        JexlNode bNode = null;

        if (queryNode instanceof AndQueryNode) {
            bNode = new JexlBooleanNode(JexlBooleanNode.Type.AND, children);
        } else if (queryNode instanceof OrQueryNode) {
            bNode = new JexlBooleanNode(JexlBooleanNode.Type.OR, children);
        } else if (queryNode instanceof NotBooleanQueryNode) {
            // NOT operator
            bNode = new JexlBooleanNode(JexlBooleanNode.Type.NOT, children);
        } else if (queryNode instanceof BooleanQueryNode) {
            // default SoftAnd operator
            bNode = new JexlBooleanNode(JexlBooleanNode.Type.AND, children);
        } else {
            throw new QueryNodeException(new MessageImpl("Unknown class: " + queryNode.getClass().getName()));
        }

        return bNode;
    }

}
