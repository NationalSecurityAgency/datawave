package datawave.query.function;

import com.google.common.base.Function;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.IndexOnlyFunctionIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.IndexOnlyJexlContext;
import datawave.query.jexl.visitors.SetMembershipVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.Tuple3;
import datawave.typemetadata.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.JexlContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates a specialized, lazy-fetching IndexOnlyJexlContext if a query includes at least one filter evaluation for an index-only field (e.g., BODY, FOOT, HEAD,
 * and META). Otherwise, it defers to its parent {@link JexlContextCreator} to create a standard {@link JexlContext}. <br>
 * <br>
 * For example, the following query would result in the creation of an ordinary {@link JexlContext}: <br>
 * 
 * <pre>
 *     REGULAR_FIELD=='value1' AND filter:includeRegex(OTHER_REGULAR_FIELD,'.*value2.*')
 * </pre>
 * 
 * <br>
 * In contrast, the following query would result in the creation of an {@link IndexOnlyJexlContext}: <br>
 * 
 * <pre>
 *     REGULAR_FIELD=='value1' AND filter:includeRegex(BODY@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION,'.*value2.*')
 * </pre>
 * 
 * <br>
 * Notice that the index-only field name must be appended with a special suffix. Such a suffix is not contained in the original query, but is added by the
 * {@link DefaultQueryPlanner} in the JBoss process when it calls the {@code IndexOnlyVisitor} as part of the process to update the JEXL query tree. <br>
 * 
 * @see IndexOnlyJexlContext
 * @see DefaultQueryPlanner
 */
public class IndexOnlyContextCreator extends JexlContextCreator {
    private final boolean createIndexOnlyJexlContext;
    private final SortedKeyValueIterator<Key,Value> documentSpecificSource;
    private final Function<Range,Key> getDocumentKey;
    private final boolean includeGroupingContext;
    private final boolean includeRecordId;
    private final Collection<String> indexOnlyFields;
    private final TypeMetadata typeMetadata;
    private final CompositeMetadata compositeMetadata;
    private final Range range;
    
    private static final String SIMPLE_NAME = IndexOnlyContextCreator.class.getSimpleName();
    private static final String SKVI_SIMPLE_NAME = SortedKeyValueIterator.class.getSimpleName();
    private static final String QO_SIMPLE_NAME = QueryOptions.class.getSimpleName();
    
    private final TimeFilter timeFilter;
    
    /**
     * Constructor
     * 
     * @param source
     *            The iterator used to fetch index-only records, as applicable, from the shard table
     * @param range
     *            The range for a given document fetched from the shard table
     * @param typeMetadata
     *            Normalizers applied to the value(s) of fields fetched for a given document
     * @param options
     *            Various query parameters and helper components
     */
    public IndexOnlyContextCreator(final SortedKeyValueIterator<Key,Value> source, final Range range, final TypeMetadata typeMetadata,
                    final CompositeMetadata compositeMetadata, final QueryOptions options, Collection<String> variables,
                    JexlContextValueComparator comparatorFactory) {
        super(variables, comparatorFactory);
        checkNotNull(source, SIMPLE_NAME + " cannot be initialized with a null " + SKVI_SIMPLE_NAME);
        checkNotNull(range, SIMPLE_NAME + " cannot be initialized with a null Range");
        checkNotNull(options, SIMPLE_NAME + " cannot be initialized with null " + QO_SIMPLE_NAME);
        this.documentSpecificSource = source;
        this.typeMetadata = typeMetadata;
        this.compositeMetadata = compositeMetadata;
        this.getDocumentKey = new CreatorOptions(options).getDocumentKey();
        this.includeGroupingContext = options.isIncludeGroupingContext();
        this.includeRecordId = options.isIncludeRecordId();
        this.indexOnlyFields = options.getIndexOnlyFields();
        
        this.range = range;
        this.timeFilter = options.getTimeFilter();
        final String query = options.getQuery();
        this.createIndexOnlyJexlContext = (null != query) && query.contains(SetMembershipVisitor.INDEX_ONLY_FUNCTION_SUFFIX);
    }
    
    public IndexOnlyContextCreator(final SortedKeyValueIterator<Key,Value> source, final Range range, final TypeMetadata typeMetadata,
                    final CompositeMetadata compositeMetadata, final QueryOptions options, JexlContextValueComparator comparatorFactory) {
        this(source, range, typeMetadata, compositeMetadata, options, Collections.<String> emptySet(), comparatorFactory);
    }
    
    @Override
    public Tuple3<Key,Document,DatawaveJexlContext> apply(Tuple3<Key,Document,Map<String,Object>> from) {
        final Tuple3<Key,Document,DatawaveJexlContext> tuple;
        if (null != from) {
            tuple = super.apply(from);
        } else {
            tuple = null;
        }
        return tuple;
    }
    
    public Function<Range,Key> getGetDocumentKey() {
        return this.getDocumentKey;
    }
    
    public Collection<String> getIndexOnlyFields() {
        if (null != this.indexOnlyFields) {
            return new HashSet<>(this.indexOnlyFields);
        } else {
            return Collections.emptySet();
        }
    }
    
    public TypeMetadata getTypeMetadata() {
        if (null != this.typeMetadata) {
            return new TypeMetadata(this.typeMetadata);
        } else {
            return new TypeMetadata();
        }
    }
    
    public CompositeMetadata getCompositeMetadata() {
        return compositeMetadata;
    }
    
    public Range getRange() {
        return this.range;
    }
    
    public TimeFilter getTimeFilter() {
        return this.timeFilter;
    }
    
    public boolean isIncludeGroupingContext() {
        return this.includeGroupingContext;
    }
    
    public boolean isIncludeRecordId() {
        return includeRecordId;
    }
    
    @Override
    protected DatawaveJexlContext newDatawaveJexlContext(final Tuple3<Key,Document,Map<String,Object>> from) {
        final DatawaveJexlContext parentContext = super.newDatawaveJexlContext(from);
        final DatawaveJexlContext newContext;
        if (this.createIndexOnlyJexlContext) {
            final Key key = from.first();
            final IndexOnlyFunctionIterator<Tuple3<Key,Document,DatawaveJexlContext>> iterator = new IndexOnlyFunctionIterator<>(this.documentSpecificSource,
                            this, key);
            newContext = new IndexOnlyJexlContext<>(parentContext, iterator);
        } else {
            newContext = parentContext;
        }
        
        return newContext;
    }
    
    /*
     * Deep copy of standard QueryOptions that adds a getter for obtaining the value of the "getDocumentKey"
     */
    private class CreatorOptions extends QueryOptions {
        public CreatorOptions(final QueryOptions other) {
            this.deepCopy(other);
        }
        
        public Function<Range,Key> getDocumentKey() {
            return super.getDocumentKey;
        }
    }
}
