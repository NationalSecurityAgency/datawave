package datawave.query.tables.facets;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SourceManager;
import datawave.query.iterator.builder.IndexIteratorBuilder;
import datawave.query.iterator.builder.IteratorBuilder;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.predicate.NegationPredicate;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.SortedKeyValueIteratorToIterator;
import datawave.query.util.TypeMetadata;
import datawave.util.UniversalSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A visitor that builds a tree of iterators. The main points are at ASTAndNodes and ASTOrNodes, where the code will build AndIterators and OrIterators,
 * respectively. This will automatically roll up binary representations of subtrees into a generic n-ary tree because there isn't a true mapping between JEXL
 * AST trees and iterator trees. A JEXL tree can have subtrees rooted at an ASTNotNode whereas an iterator tree cannot.
 * 
 */
public class FacetedVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(FacetedVisitor.class);
    
    public static final String NULL_DELIMETER = "\u0000";
    
    @SuppressWarnings("rawtypes")
    protected NestedIterator root;
    protected SourceManager source;
    protected SortedKeyValueIterator<Key,Value> limitedSource = null;
    protected Map<Entry<String,String>,Entry<Key,Value>> limitedMap = null;
    protected IteratorEnvironment env;
    protected Collection<String> includeReferences = UniversalSet.instance();
    protected Collection<String> excludeReferences = Collections.emptyList();
    protected Predicate<Key> datatypeFilter;
    protected TimeFilter timeFilter;
    
    protected TypeMetadata typeMetadata;
    
    protected Range rangeLimiter;
    
    protected boolean limitLookup;
    
    protected Class<? extends IteratorBuilder> iteratorBuilderClass = IndexIteratorBuilder.class;
    
    private Collection<String> unindexedFields = Lists.newArrayList();
    
    protected NegationPredicate predicate = null;
    
    private Set<String> fieldFacets;
    
    private String row;
    
    private FacetTableFunction tableFunction;
    
    public FacetedVisitor(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env, TimeFilter timeFilter, Predicate<Key> datatypeFilter,
                    TypeMetadata typeMetadata, Set<String> fieldFacets, String row) {
        this.source = new SourceManager(source);
        Map<String,String> options = Maps.newHashMap();
        try {
            this.source.init(source, options, env);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.env = env;
        this.timeFilter = timeFilter;
        this.typeMetadata = typeMetadata;
        this.datatypeFilter = datatypeFilter;
        this.fieldFacets = fieldFacets;
        this.row = row;
        tableFunction = new FacetTableFunction();
    }
    
    @SuppressWarnings("unchecked")
    public <T> NestedIterator<T> root() {
        return root;
    }
    
    public Iterator<Entry<Key,Document>> streamFacets(ASTJexlScript script, String shard)
    
    {
        
        List<Iterator<Entry<Key,Value>>> kvIterList = Lists.newArrayList();
        
        script.jjtAccept(this, kvIterList);
        
        Iterator<Entry<Key,Value>> kvIter = Collections.emptyIterator();
        for (Iterator<Entry<Key,Value>> iter : kvIterList) {
            kvIter = Iterators.concat(kvIter, iter);
        }
        
        return Iterators.transform(kvIter, tableFunction);
        
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        
        /*
         * final String fieldName = JexlASTHelper.getIdentifier(node);
         * 
         * // Row : shardId ColumnFamily : PIVOT_FIELD\x00FACET_FIELD ColumnQualifier : PIVOT_VALUE\x00FACET_VALUE\x00DATA_TYPE ColumnVisibility : [VIZ] Value
         * // : // [HLL]
         * 
         * final Object literal = JexlASTHelper.getLiteralValue(node);
         * 
         * List<Iterator<Entry<Key,Value>>> kvIterList = (List<Iterator<Entry<Key,Value>>>) data;
         * 
         * // Row : shardId ColumnFamily : PIVOT_FIELD\x00FACET_FIELD ColumnQualifier : PIVOT_VALUE\x00FACET_VALUE\x00DATA_TYPE ColumnVisibility : [VIZ] Value
         * // : // [HLL]
         * 
         * if (null != literal) { final String stringLiteral = literal.toString(); for (String facetField : fieldFacets) { StringBuilder cf = new
         * StringBuilder(fieldName); cf.append("\u0000").append(facetField); Key startKey = new Key(new Text(row), new Text(cf.toString()), new
         * Text(stringLiteral + "\u0000")); Key endKey = new Key(new Text(row), new Text(cf.toString()), new Text(stringLiteral + "\u1111")); try {
         * 
         * source.seek(new Range(startKey, true, endKey, false), Collections.<ByteSequence> emptyList(), false); kvIterList.add(new
         * SortedKeyValueIteratorToIterator(source));
         * 
         * } catch (IOException e) { throw new RuntimeException(e); } } }
         */
        final String fieldName = JexlASTHelper.getIdentifier(node);
        
        // Row : PIVOT_VALUE\x00FACET_VALUE ColumnFamily : PIVOT_FIELD\x00FACET_FIELD ColumnQualifier : DATE ColumnVisibility : [VIZ] Value
        // :
        // [HLL]
        
        final Object literal = JexlASTHelper.getLiteralValue(node);
        
        List<Iterator<Entry<Key,Value>>> kvIterList = (List<Iterator<Entry<Key,Value>>>) data;
        
        if (null != literal) {
            final String stringLiteral = literal.toString();
            for (String facetField : fieldFacets) {
                StringBuilder cf = new StringBuilder(fieldName);
                cf.append("\u0000").append(facetField);
                Key startKey = new Key(new Text(row), new Text(cf.toString()), new Text(stringLiteral + "\u0000"));
                Key endKey = new Key(new Text(row), new Text(cf.toString()), new Text(stringLiteral + "\u1111"));
                try {
                    
                    source.seek(new Range(startKey, true, endKey, false), Collections.emptyList(), false);
                    kvIterList.add(new SortedKeyValueIteratorToIterator(source));
                    
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
    
    public FacetedVisitor setRange(Range documentRange) {
        this.rangeLimiter = documentRange;
        return this;
    }
    
    public FacetedVisitor limit(Range documentRange) {
        return setRange(documentRange).setLimitLookup(true);
    }
    
    /**
     * Limits the number of source counts.
     * 
     * @param sourceCount
     *            the source count
     * @return the current visitor
     */
    public FacetedVisitor limit(long sourceCount) {
        source.setInitialSize(sourceCount);
        return this;
    }
    
    public FacetedVisitor setLimitLookup(boolean limitLookup) {
        if (rangeLimiter != null) {
            this.limitLookup = limitLookup;
        }
        return this;
    }
    
    public FacetedVisitor setIteratorBuilder(Class<? extends IteratorBuilder> clazz) {
        this.iteratorBuilderClass = clazz;
        return this;
    }
    
    protected boolean isUnindexed(ASTEQNode node) {
        final String fieldName = JexlASTHelper.getIdentifier(node);
        return unindexedFields.contains(fieldName);
    }
    
    protected boolean isUnindexed(ASTIdentifier node) {
        final String fieldName = JexlASTHelper.deconstructIdentifier(node.getName());
        return unindexedFields.contains(fieldName);
    }
    
    public FacetedVisitor setUnindexedFields(Collection<String> unindexedField) {
        this.unindexedFields.addAll(unindexedField);
        return this;
    }
    
}
