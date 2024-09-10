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
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

/**
 * Builds no object, it only returns the {@link Query} object set on the {@link ModifierQueryNode} object using a
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} tag.
 */
@Deprecated
public class ModifierQueryNodeBuilder implements QueryBuilder {

    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        ModifierQueryNode modifierQueryNode = (ModifierQueryNode) queryNode;

        return (datawave.query.language.tree.QueryNode) (modifierQueryNode).getChild().getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    }
}
