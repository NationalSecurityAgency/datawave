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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.query.language.functions.lucene.EvaluationOnly;
import datawave.query.language.functions.lucene.Exclude;
import datawave.query.language.functions.lucene.Include;
import datawave.query.language.functions.lucene.IsNotNull;
import datawave.query.language.functions.lucene.IsNull;
import datawave.query.language.functions.lucene.LuceneQueryFunction;
import datawave.query.language.functions.lucene.Occurrence;
import datawave.query.language.functions.lucene.Text;
import datawave.query.language.tree.FunctionNode;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.TermQuery;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
@Deprecated
public class FunctionQueryNodeBuilder implements QueryBuilder {
    
    private Map<String,LuceneQueryFunction> allowedFunctionMap = Collections.synchronizedMap(new HashMap<>());
    
    public FunctionQueryNodeBuilder() {
        addFunction(new IsNull());
        addFunction(new IsNotNull());
        addFunction(new Include());
        addFunction(new Exclude());
        addFunction(new Text());
        addFunction(new Occurrence());
        addFunction(new EvaluationOnly());
    }
    
    public FunctionQueryNodeBuilder(List<LuceneQueryFunction> allowedFunctions) {
        setAllowedFunctions(allowedFunctions);
    }
    
    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        datawave.query.language.tree.QueryNode returnNode = null;
        int depth = 0;
        QueryNode parent = queryNode;
        while ((parent = parent.getParent()) != null) {
            depth++;
        }
        
        if (queryNode instanceof FunctionQueryNode) {
            FunctionQueryNode functionQueryNode = (FunctionQueryNode) queryNode;
            
            String functionName = functionQueryNode.getFunction();
            List<String> parameterList = functionQueryNode.getParameterList();
            
            LuceneQueryFunction referenceFunction = allowedFunctionMap.get(functionName.toUpperCase());
            
            if (referenceFunction == null) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND, MessageFormat.format("{0}", functionName));
                throw new IllegalArgumentException(qe);
            }
            
            LuceneQueryFunction function = (LuceneQueryFunction) referenceFunction.duplicate();
            
            returnNode = new FunctionNode(function, parameterList, depth, queryNode.getParent());
        }
        
        return returnNode;
    }
    
    public List<LuceneQueryFunction> getAllowedFunctions() {
        List<LuceneQueryFunction> allowedFunctions = new ArrayList<>();
        allowedFunctions.addAll(allowedFunctionMap.values());
        return allowedFunctions;
    }
    
    public void setAllowedFunctions(List<LuceneQueryFunction> allowedFunctions) {
        allowedFunctionMap.clear();
        for (LuceneQueryFunction f : allowedFunctions) {
            addFunction(f);
        }
    }
    
    private void addFunction(LuceneQueryFunction function) {
        allowedFunctionMap.put(function.getName().toUpperCase(), function);
    }
}
