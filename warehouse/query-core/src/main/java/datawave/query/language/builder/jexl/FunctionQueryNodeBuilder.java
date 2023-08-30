package datawave.query.language.builder.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
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
import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.parser.jexl.JexlFunctionNode;
import datawave.query.language.parser.jexl.JexlNode;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
public class FunctionQueryNodeBuilder implements QueryBuilder {

    private Map<String,JexlQueryFunction> allowedFunctionMap = Collections.synchronizedMap(new HashMap<>());

    public FunctionQueryNodeBuilder(List<JexlQueryFunction> allowedFunctions) {
        setAllowedFunctions(allowedFunctions);
    }

    public JexlNode build(QueryNode queryNode) {
        JexlNode returnNode = null;

        int depth = 0;
        QueryNode parent = queryNode;
        while ((parent = parent.getParent()) != null) {
            depth++;
        }

        if (queryNode instanceof FunctionQueryNode) {
            FunctionQueryNode functionQueryNode = (FunctionQueryNode) queryNode;

            String functionName = functionQueryNode.getFunction();
            List<String> parameterList = functionQueryNode.getParameterList();

            JexlQueryFunction referenceFunction = allowedFunctionMap.get(functionName.toUpperCase());

            if (referenceFunction == null) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND, MessageFormat.format("{0}", functionName));
                throw new IllegalArgumentException(qe);
            }
            // if more than one term in quotes, use an AdjNode
            JexlQueryFunction function = (JexlQueryFunction) referenceFunction.duplicate();

            returnNode = new JexlFunctionNode(function, parameterList, depth, queryNode.getParent());
        }

        return returnNode;
    }

    public List<JexlQueryFunction> getAllowedFunctions() {
        List<JexlQueryFunction> allowedFunctions = new ArrayList<>();
        allowedFunctions.addAll(allowedFunctionMap.values());
        return allowedFunctions;
    }

    public void setAllowedFunctions(List<JexlQueryFunction> allowedFunctions) {
        allowedFunctionMap.clear();
        for (JexlQueryFunction f : allowedFunctions) {
            addFunction(f);
        }
    }

    private void addFunction(JexlQueryFunction function) {
        allowedFunctionMap.put(function.getName().toUpperCase(), function);
    }
}
