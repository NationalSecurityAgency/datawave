package datawave.query.function.ws;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;

import com.google.common.base.Function;

import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple3;

/**
 * A function that performs document evaluation in the webservice.
 *
 * This function is initialized lazily in order to have the final query at the end of query planning.
 */
public class EvaluationFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    protected ShardQueryConfiguration config;
    protected Set<String> queryFields;
    protected Set<String> incompleteFields;
    protected JexlEvaluation evaluation;

    // for lazy init
    private boolean useHitListArithmetic = false;

    /**
     * Use the config to access the JexlScript, query fields, and configured incompleteFields
     *
     * @param config
     *            a {@link ShardQueryConfiguration}
     */
    public EvaluationFunction(ShardQueryConfiguration config) {
        this.config = config;
        this.incompleteFields = config.getIncompleteFields();
        this.useHitListArithmetic = config.isHitList();

        // this solves the problem of not having a full query tree when the evaluating transform is setup first
        config.setEvaluationFunction(this);
    }

    /**
     *
     *
     * @param query
     *            the query string after query planning, before hitting the range stream. Any further permutation is derivative
     */
    public void lazyInitialize(String query) {
        config.setQueryString(query);
        queryFields = populateQueryFields(query);
        if (useHitListArithmetic) {
            // building a hit list arithmetic
            evaluation = new JexlEvaluation(query, new HitListArithmetic());
        } else {
            evaluation = new JexlEvaluation(query, new DefaultArithmetic());
        }
    }

    public String getUpdatedQueryString() {
        return config.getQueryString();
    }

    /**
     * Evaluates the document
     *
     * @param input
     *            a Document
     * @return the Document if it evaluated to true, false otherwise
     */
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        if (input.getValue().containsKey(JexlEvaluation.EVAL_STATE_FIELD)) {

            // remove the marker
            input.getValue().remove(JexlEvaluation.EVAL_STATE_FIELD);

            Tuple3<Key,Document,DatawaveJexlContext> transformed = transformInput(input);
            if (!evaluation.apply(transformed)) {
                input = null; // null out document that fails evaluation
            }
        }
        return input;
    }

    /**
     * Transforms a Key,Document entry into a tuple of 'Key,Document,Context'
     *
     * @param input
     *            a 'Key,Document' entry
     * @return a 'Key,Document,Context' entry
     */
    private Tuple3<Key,Document,DatawaveJexlContext> transformInput(Map.Entry<Key,Document> input) {
        DatawaveJexlContext context = buildContextFromInput(input);
        return new Tuple3<>(input.getKey(), input.getValue(), context);
    }

    /**
     * Build a JexlContext from the document, limiting the input to the fields found in the query
     *
     * @param input
     *            a map entry of 'Key, Document'
     * @return a populated JexlContext
     */
    private DatawaveJexlContext buildContextFromInput(Map.Entry<Key,Document> input) {
        DatawaveJexlContext context = new DatawaveJexlContext();
        Document d = input.getValue();
        d.visit(queryFields, context);
        if (d.getOffsetMap() != null && !d.getOffsetMap().getTermFrequencyKeySet().isEmpty()) {
            context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, d.getOffsetMap());
        }
        return context;
    }

    /**
     * Extract all fields into a set.
     *
     * @param query
     *            the raw query string
     * @return all fields from the query
     */
    protected Set<String> populateQueryFields(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

            // it might be worth filtering out any constants that slip through...
            return JexlASTHelper.getIdentifierNames(script);
        } catch (ParseException e) {
            e.printStackTrace();
            // ignore. in reality, an invalid query tree would have thrown a parse exception far earlier
        }
        return Collections.emptySet();
    }
}
