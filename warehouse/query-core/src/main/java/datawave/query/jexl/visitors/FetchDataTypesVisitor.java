package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;
import datawave.query.util.Tuple2;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Walk the AST returning a Multimap of String fieldNames to Sets of Types for terms in the query that need normalization for the express purpose of index
 * lookups. Meaning, we don't need to get normalizers to fields which aren't use in index lookups (e.g. FIELD1 op FIELD2, some functions, etc)
 * 
 */
@SuppressWarnings("unchecked")
public class FetchDataTypesVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(FetchDataTypesVisitor.class);
    
    protected final MetadataHelper helper;
    protected final Set<String> datatypeFilter;
    protected boolean useCache;
    
    protected static final Cache<Tuple2<String,Set<String>>,Set<Type<?>>> typeCache = CacheBuilder.newBuilder().maximumSize(2000).concurrencyLevel(25)
                    .expireAfterWrite(24, TimeUnit.HOURS).build();
    
    public FetchDataTypesVisitor(MetadataHelper helper, Set<String> datatypeFilter, boolean useCache) {
        this.helper = helper;
        this.datatypeFilter = datatypeFilter;
        this.useCache = useCache;
    }
    
    public FetchDataTypesVisitor(MetadataHelper helper, Set<String> datatypeFilter) {
        this(helper, datatypeFilter, false);
    }
    
    public static HashMultimap<String,Type<?>> fetchDataTypes(MetadataHelper helper, Set<String> datatypeFilter, ASTJexlScript script, boolean useCache) {
        FetchDataTypesVisitor visitor = new FetchDataTypesVisitor(helper, datatypeFilter, useCache);
        
        return (HashMultimap<String,Type<?>>) script.jjtAccept(visitor, HashMultimap.create());
    }
    
    /**
     * Use the MetadataHelper to fetch the Set&lt;Type&gt;'s for each field specified in a query term. Handle the case of spoofing the NumberType for fields
     * which are numeric but not indexed.
     * 
     * @param node
     *            a node
     * @param data
     *            the data for the node
     * @return the data visited
     */
    private Object genericVisit(JexlNode node, Object data) {
        HashMultimap<String,Type<?>> dataTypes = (HashMultimap<String,Type<?>>) data;
        
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return dataTypes;
        }
        
        final String fieldName = op.deconstructIdentifier();
        
        if (!dataTypes.containsKey(fieldName)) {
            Set<Type<?>> dataTypesForField = Collections.emptySet();
            
            try {
                if (useCache) {
                    Tuple2<String,Set<String>> cacheKey = new Tuple2<>(fieldName, datatypeFilter);
                    Set<Type<?>> types = typeCache.getIfPresent(cacheKey);
                    if (null == types) {
                        dataTypesForField = this.helper.getDatatypesForField(fieldName, datatypeFilter);
                        typeCache.put(cacheKey, dataTypesForField);
                    } else {
                        dataTypesForField = types;
                    }
                    
                } else
                    dataTypesForField = this.helper.getDatatypesForField(fieldName, datatypeFilter);
            } catch (InstantiationException | TableNotFoundException | IllegalAccessException e) {
                log.error(e);
            }
            
            if (!dataTypesForField.isEmpty()) {
                dataTypes.putAll(fieldName, dataTypesForField);
            } else {
                if (op.getLiteralValue() instanceof Number) {
                    // This is a hack to get around the following case:
                    // 1) A user enters a query with a numeric term on a field
                    // that isn't indexed: e.g. AGE < 4
                    // 2) To get proper comparisons during evaluation, the
                    // NumberType needs to be
                    // set on the AGE, otherwise lexicographic comparisons will
                    // occur.
                    // This causes a problem though, because the
                    // RangeBuildingVisitor thinks that
                    // AGE is indexed, it is not, so it incorrectly fails
                    // queries where "AGE < 4" is
                    // intersected with an indexed field.
                    // If this is unindexed (no normalizers for it) and the
                    // literal is a Number
                    dataTypes.put(fieldName, new NumberType());
                    if (log.isTraceEnabled()) {
                        log.trace("Unindexed numeric field, adding NumberType for " + fieldName);
                    }
                } else {
                    // add LcNoDiacritics and NoOpType to ensure that we
                    // query against both forms of the string
                    dataTypes.put(fieldName, new LcNoDiacriticsType());
                    dataTypes.put(fieldName, new NoOpType());
                    if (log.isTraceEnabled()) {
                        log.trace("Unindexed field, adding LcNoDiacriticsType and NoOpType for " + fieldName);
                    }
                }
            }
        }
        
        return dataTypes;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        Multimap<String,Type<?>> mm = (Multimap<String,Type<?>>) data;
        for (String field : desc.fields(this.helper, this.datatypeFilter)) {
            final String fieldName = JexlASTHelper.deconstructIdentifier(field);
            try {
                
                Set<Type<?>> dataTypesForField = Collections.emptySet();
                
                if (useCache) {
                    
                    Tuple2<String,Set<String>> cacheKey = new Tuple2<>(fieldName, datatypeFilter);
                    Set<Type<?>> types = typeCache.getIfPresent(cacheKey);
                    if (null == types) {
                        dataTypesForField = this.helper.getDatatypesForField(fieldName, datatypeFilter);
                        typeCache.put(cacheKey, dataTypesForField);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("using cached types for " + fieldName + " " + datatypeFilter);
                        }
                        dataTypesForField = types;
                    }
                } else
                    dataTypesForField = this.helper.getDatatypesForField(fieldName, datatypeFilter);
                
                mm.putAll(field, dataTypesForField);
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            } catch (InstantiationException | IllegalAccessException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_RECORD_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }
        return data;
    }
    
    public static Multimap<String,Type<?>> fetchDataTypes(MetadataHelper metadataHelper, Set<String> datatypeFilter, ASTJexlScript queryTree) {
        return fetchDataTypes(metadataHelper, datatypeFilter, queryTree, false);
    }
}
