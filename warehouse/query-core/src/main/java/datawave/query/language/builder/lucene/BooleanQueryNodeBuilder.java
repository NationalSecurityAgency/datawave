package datawave.query.language.builder.lucene;

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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import datawave.query.language.tree.HardAndNode;
import datawave.query.language.tree.NotNode;
import datawave.query.language.tree.OrNode;
import datawave.query.language.tree.SoftAndNode;

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
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object. Every children in the {@link BooleanQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object. <br>
 * <br>
 * It takes in consideration if the children is a {@link ModifierQueryNode} to define the {@link BooleanClause}.
 */
@Deprecated
public class BooleanQueryNodeBuilder implements QueryBuilder {
    
    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;
        
        datawave.query.language.tree.QueryNode bNode = null;
        List<QueryNode> children = booleanNode.getChildren();
        
        if (children != null) {
            List<datawave.query.language.tree.QueryNode> childrenList = new ArrayList<>();
            
            LinkedList<QueryNode> extraNodeList = new LinkedList<>();
            boolean isNegation = false;
            
            for (QueryNode child : children) {
                Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
                if (obj != null) {
                    datawave.query.language.tree.QueryNode query = (datawave.query.language.tree.QueryNode) obj;
                    childrenList.add(query);
                }
            }
            
            datawave.query.language.tree.QueryNode[] childrenArray = new datawave.query.language.tree.QueryNode[childrenList.size()];
            childrenList.toArray(childrenArray);
            
            bNode = createNode(queryNode, childrenArray, isNegation, extraNodeList);
        }
        
        return bNode;
        
    }
    
    private datawave.query.language.tree.QueryNode createNode(QueryNode queryNode, datawave.query.language.tree.QueryNode[] childrenArray, boolean isNegation,
                    LinkedList<QueryNode> extraNodeList) throws QueryNodeException {
        datawave.query.language.tree.QueryNode bNode = null;
        
        if (queryNode instanceof AndQueryNode) {
            bNode = new HardAndNode(childrenArray);
        } else if (queryNode instanceof OrQueryNode) {
            bNode = new OrNode(childrenArray);
        } else if (queryNode instanceof NotBooleanQueryNode) {
            // NOT operator
            bNode = new NotNode(childrenArray);
        } else if (queryNode instanceof BooleanQueryNode) {
            // default SoftAnd operator
            bNode = new SoftAndNode(childrenArray);
        } else {
            throw new QueryNodeException(new MessageImpl("Unknown class: " + queryNode.getClass().getName()));
        }
        
        return bNode;
    }
    
}
