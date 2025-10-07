package datawave.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
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
import datawave.query.language.parser.jexl.JexlNode;
import datawave.query.language.parser.jexl.JexlPhraseNode;
import datawave.query.language.parser.jexl.JexlSelectorNode;
import datawave.query.language.parser.jexl.JexlWithinNode;

/**
 * This builder basically reads the {@link Query} object set on the {@link SlopQueryNode} child using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and
 * applies the slop value defined in the {@link SlopQueryNode}.
 */
public class SlopQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        JexlNode returnNode = null;

        SlopQueryNode phraseSlopNode = (SlopQueryNode) queryNode;

        JexlNode node = (JexlNode) phraseSlopNode.getChild().getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (node instanceof JexlPhraseNode) {
            JexlPhraseNode phraseNode = (JexlPhraseNode) node;
            returnNode = new JexlWithinNode(phraseNode.getField(), phraseNode.getWordList(), phraseSlopNode.getValue());
        } else if (node instanceof JexlSelectorNode) {
            // if phrase only contained one word, a JexlSelectorNode would be created
            // and then a SlopQueryNode / within makes no sense
            returnNode = node;
        } else {
            throw new UnsupportedOperationException(node.getClass().getName() + " found as a child of a SlopQueryNode -- not implemented");
        }

        return returnNode;
    }

}
