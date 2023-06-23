package datawave.query.jexl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import datawave.query.iterator.IndexOnlyFunctionIterator;
import static datawave.query.jexl.visitors.SetMembershipVisitor.INDEX_ONLY_FUNCTION_SUFFIX;
import static datawave.query.Constants.EMPTY_STRING;

import org.apache.commons.jexl2.MapContext;

import com.google.common.base.Objects;

/**
 * A map context tied into an iterator for fetching and performing functions on index-only fields
 *
 * @param <T>
 *            The type of input
 */
public class IndexOnlyJexlContext<T> extends DatawaveJexlContext {
    private final Set<String> indexOnlyFields;
    private final IndexOnlyFunctionIterator<T> iterator;
    private final Map<String,WeakReference<Collection<?>>> lazyFetchingIndexOnlySets;
    private final MapContext parent;

    /**
     * Constructor
     *
     * @param context
     *            A non-null MapContext, presumably an instance of {@link DatawaveJexlContext}
     * @param iterator
     *            A non-null IndexOnlyFunctionIterator
     */
    public IndexOnlyJexlContext(final MapContext context, final IndexOnlyFunctionIterator<T> iterator) {
        checkNotNull(context, this.getClass().getSimpleName() + " cannot be initialized with a null " + MapContext.class.getSimpleName());
        checkNotNull(iterator, this.getClass().getSimpleName() + " cannot be initialized with a null " + IndexOnlyFunctionIterator.class.getSimpleName());
        this.parent = context;
        this.iterator = iterator;

        // Create a collection of index-only field names for quick lookup
        final Collection<String> fields = this.iterator.getIndexOnlyFields();
        if (null != fields) {
            this.indexOnlyFields = Collections.unmodifiableSet(new HashSet<>(fields));
        } else {
            this.indexOnlyFields = Collections.emptySet();
        }

        // Create a weak hashmap to allow garbage collection in case of abnormally large sets of index-only records, yet prevent duplicate
        // fetching if the values can be held in memory.
        this.lazyFetchingIndexOnlySets = Collections.synchronizedMap(new WeakHashMap<>());
    }

    @Override
    public Object get(final String name) {
        // Get the value from the parent context, if defined.
        final Object value;

        // Otherwise, consider using index-only value(s) fetched from the fi section of the shard
        // table, if applicable
        if ((null != name) && (name.endsWith(INDEX_ONLY_FUNCTION_SUFFIX))) {
            final String baseName = name.replace(INDEX_ONLY_FUNCTION_SUFFIX, EMPTY_STRING);
            if (this.indexOnlyFields.contains(baseName)) {
                value = this.getLazyFetchingSet(baseName);
            } else {
                value = this.parent.get(name);
            }
        }
        // Get the value from the parent context, if defined.
        else {
            value = this.parent.get(name);
        }

        return value;
    }

    @Override
    public boolean has(final String name) {
        boolean hasField = this.parent.has(name);
        if (!hasField && (null != name) && (name.endsWith(INDEX_ONLY_FUNCTION_SUFFIX))) {
            final String baseName = name.replace(INDEX_ONLY_FUNCTION_SUFFIX, EMPTY_STRING);
            if (this.indexOnlyFields.contains(baseName)) {
                final Collection<?> lazyFetchingSet = this.getLazyFetchingSet(baseName);
                if (null != lazyFetchingSet) {
                    hasField = !lazyFetchingSet.isEmpty();
                }
            }
        }

        return hasField;
    }

    /**
     * Returns a collection of index-only field names
     *
     * @return a collection of index-only field names
     */
    public Collection<String> getIndexOnlyFields() {
        return Collections.unmodifiableSet(this.indexOnlyFields);
    }

    /**
     * Returns the iterator the context will use to fetch index-only records
     *
     * @return the iterator the context will use to fetch index-only records
     */
    public IndexOnlyFunctionIterator<T> getIterator() {
        return this.iterator;
    }

    /*
     * Lookup or generate a JEXL context based on an index-only field name
     *
     * @param name A presumably index-only field name
     *
     * @return An in-memory or newly generated context
     */
    private Collection<?> getLazyFetchingSet(final String name) {
        final Collection<?> lazyFetchingSet;
        if ((null != name) && !name.isEmpty() && this.indexOnlyFields.contains(name)) {
            // See if there's a set already available in memory
            final WeakReference<Collection<?>> ref = this.lazyFetchingIndexOnlySets.get(name);
            final Collection<?> values;
            if (null != ref) {
                values = ref.get();
            } else {
                values = null;
            }

            // Use the set obtained from memory, or create a new one. Fetching won't occur until the
            // set is used, such as asking for its size, iterator, etc.
            if (null != values) {
                lazyFetchingSet = values;
            } else {
                lazyFetchingSet = Collections.synchronizedSet(new IndexOnlyLazyFetchingSet<>(name, this.iterator));
                this.lazyFetchingIndexOnlySets.put(name, new WeakReference<>(lazyFetchingSet));
            }
        } else {
            lazyFetchingSet = null;
        }

        return lazyFetchingSet;
    }

    @Override
    public void set(final String name, Object value) {
        this.parent.set(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IndexOnlyJexlContext)) {
            return false;
        }
        IndexOnlyJexlContext other = (IndexOnlyJexlContext) o;
        return super.equals(other) && Objects.equal(indexOnlyFields, other.indexOnlyFields) && Objects.equal(parent, other.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), indexOnlyFields, parent);
    }
}
