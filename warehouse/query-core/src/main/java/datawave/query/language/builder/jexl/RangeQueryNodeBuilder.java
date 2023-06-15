package datawave.query.language.builder.jexl;

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
import datawave.query.language.parser.jexl.JexlRangeNode;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.search.TermRangeQuery;

/**
 * Builds a {@link TermRangeQuery} object from a {@link RangeQueryNode} object.
 */
public class RangeQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        TermRangeQueryNode rangeNode = (TermRangeQueryNode) queryNode;
        FieldQueryNode lower = rangeNode.getLowerBound();
        FieldQueryNode upper = rangeNode.getUpperBound();

        boolean lowerWildcard = lower.getTextAsString().equals("*");
        boolean upperWildcard = upper.getTextAsString().equals("*");
        if (lowerWildcard && upperWildcard) {
            throw new QueryNodeException(new MessageImpl("Wildcard of lower and upper bouds not allowed"));
        }

        String field = rangeNode.getField().toString();

        return new JexlRangeNode(field, lower.getTextAsString(), upper.getTextAsString(), rangeNode.isLowerInclusive(), rangeNode.isUpperInclusive());
    }

}
