package datawave.query.tables.facets;

import com.google.common.collect.Multimap;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTEQNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class FacetQueryPlanVisitor extends BaseVisitor implements CloseableIterable<QueryPlan> {

    protected Multimap<String,String> facetMultimap;
    protected ShardQueryConfiguration config;
    protected Set<QueryPlan> queryPlans;
    protected Set<String> facetedFields;

    public FacetQueryPlanVisitor(ShardQueryConfiguration config, FacetedConfiguration facetedConfig, MetadataHelper helper, Set<String> facetedFields) {

        this.config = config;
        queryPlans = new HashSet<>();
        this.facetedFields = new HashSet<>();
        this.facetedFields.addAll(facetedFields);
        try {
            facetMultimap = helper.getFacets(facetedConfig.getFacetMetadataTableName());
        } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public QueryPlan visit(ASTEQNode node, Object data) {

        // We are looking for identifier = literal
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return null;
        }

        final String fieldName = op.deconstructIdentifier();

        // if we have a null literal, then we cannot resolve against the index
        if (op.getLiteralValue() == null) {
            return null;
        }

        String literal = op.getLiteralValue().toString();

        Key startKey = new Key(literal + "\u0000");
        Key endKey = new Key(literal + "\uFFFF");

        Collection<String> fieldPairs = new ArrayList<>();
        for (String facet : facetedFields) {
            StringBuilder facetBuilder = new StringBuilder(fieldName);
            facetBuilder.append("\u0000").append(facet);
            fieldPairs.add(facetBuilder.toString());
        }

        QueryPlan plan = new QueryPlan(config.getShardTableName(), node, Collections.singleton(new Range(startKey, true, endKey, false)), fieldPairs);
        // toString of String returns the String
        queryPlans.add(plan);
        return plan;

    }

    @Override
    public Iterator<QueryPlan> iterator() {
        return queryPlans.iterator();
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

}
