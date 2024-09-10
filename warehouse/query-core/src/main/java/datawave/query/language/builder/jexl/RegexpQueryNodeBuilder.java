package datawave.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.search.TermQuery;

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
import datawave.query.language.parser.jexl.JexlSelectorNode;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
public class RegexpQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        JexlNode returnNode = null;

        if (queryNode instanceof RegexpQueryNode) {
            RegexpQueryNode regexpQueryNode = (RegexpQueryNode) queryNode;
            String field = regexpQueryNode.getFieldAsString();
            UnescapedCharSequence ecs = (UnescapedCharSequence) regexpQueryNode.getText();

            if (field == null || field.isEmpty()) {
                returnNode = new JexlSelectorNode(JexlSelectorNode.Type.REGEX, "", ecs.toString());
            } else {
                returnNode = new JexlSelectorNode(JexlSelectorNode.Type.REGEX, field, ecs.toString());
            }
        }

        return returnNode;
    }

}
