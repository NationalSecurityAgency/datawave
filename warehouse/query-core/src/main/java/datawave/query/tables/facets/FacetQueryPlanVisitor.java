package datawave.query.tables.facets;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import datawave.query.CloseableIterable;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.config.RefactoredShardQueryConfiguration;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.util.MetadataHelper;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTEQNode;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Multimap;

public class FacetQueryPlanVisitor extends BaseVisitor implements CloseableIterable<QueryPlan> {
    
    protected Multimap<String,String> facetMultimap;
    protected RefactoredShardQueryConfiguration config;
    protected Set<QueryPlan> queryPlans;
    protected Set<String> facetedFields;
    
    public FacetQueryPlanVisitor(RefactoredShardQueryConfiguration config, MetadataHelper helper, Set<String> facetedFields) {
        
        this.config = config;
        queryPlans = Sets.newHashSet();
        this.facetedFields = Sets.newHashSet();
        this.facetedFields.addAll(facetedFields);
        try {
            facetMultimap = helper.getFacets("FacetsNatingMetadata");
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
        
        Collection<String> fieldPairs = Lists.newArrayList();
        for (String facet : facetedFields) {
            StringBuilder facetBuilder = new StringBuilder(fieldName);
            facetBuilder.append("\u0000").append(facet);
            fieldPairs.add(facetBuilder.toString());
        }
        
        QueryPlan plan = new QueryPlan(node, Collections.singleton(new Range(startKey, true, endKey, false)), fieldPairs);
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
