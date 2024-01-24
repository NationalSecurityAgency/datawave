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
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.search.PhraseQuery;

import datawave.query.language.tree.AdjNode;
import datawave.query.language.tree.SelectorNode;

/**
 * Builds a {@link PhraseQuery} object from a {@link TokenizedPhraseQueryNode} object.
 */
@Deprecated
public class PhraseQueryNodeBuilder implements QueryBuilder {

    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        TokenizedPhraseQueryNode phraseNode = (TokenizedPhraseQueryNode) queryNode;
        datawave.query.language.tree.QueryNode bNode = null;

        List<QueryNode> children = phraseNode.getChildren();
        List<datawave.query.language.tree.QueryNode> childrenList = new ArrayList<>();

        if (children != null) {

            for (QueryNode child : children) {
                SelectorNode selectorNode = (SelectorNode) child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
                childrenList.add(selectorNode);
            }
        }

        if (children != null) {
            datawave.query.language.tree.QueryNode[] childrenArray = new datawave.query.language.tree.QueryNode[childrenList.size()];
            childrenList.toArray(childrenArray);
            bNode = new AdjNode((childrenArray.length - 1), childrenArray);
        } else {
            throw new QueryNodeException(new MessageImpl("Unknown class: " + queryNode.getClass().getName()));
        }

        return bNode;
    }
}
