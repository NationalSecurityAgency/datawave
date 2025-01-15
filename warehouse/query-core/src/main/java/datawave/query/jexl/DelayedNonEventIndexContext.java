package datawave.query.jexl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Objects;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Document;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.function.Equality;
import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;

/**
 * Responsible for retrieving delayed fields for the specified docRange on demand and merging it with any values already in the delegate. Use the
 * IteratorBuildingVisitor on each delayed sub tree to generate iterators over the docRange.
 */
public class DelayedNonEventIndexContext extends DatawaveJexlContext {
    private DatawaveJexlContext delegate;
    private IteratorBuildingVisitor iteratorBuildingVisitor;
    private Multimap<String,JexlNode> delayedNonEventFieldMap;
    private Range docRange;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;
    private Equality equality;

    /**
     * track which fields have been fetched already
     */
    private Set<String> fetched;

    public DelayedNonEventIndexContext(DatawaveJexlContext delegate, IteratorBuildingVisitor iteratorBuildingVisitor,
                    Multimap<String,JexlNode> delayedNonEventFieldMap, Range docRange, Collection<ByteSequence> columnFamilies, boolean inclusive,
                    Equality equality) {
        this.delegate = delegate;
        this.iteratorBuildingVisitor = iteratorBuildingVisitor;
        this.delayedNonEventFieldMap = delayedNonEventFieldMap;
        this.docRange = docRange;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        this.equality = equality;

        fetched = new HashSet<>();
    }

    @Override
    public void set(String name, Object value) {
        delegate.set(name, value);
    }

    @Override
    public Object get(String name) {
        // only do something special if there is delayed work to do
        if ((null != name) && delayedNonEventFieldMap.containsKey(name) && !fetched.contains(name)) {
            // fetch the field that was delayed
            List<Document> documentFragments = null;
            try {
                documentFragments = fetchOnDemand(name);
            } catch (IOException e) {
                throw new RuntimeException("Failed to fetch delayed index only fragments for field: " + name, e);
            }

            // aggregate all of that into the delegate
            for (Document document : documentFragments) {
                document.visit(Collections.singleton(name), delegate);
            }

            // flag that this field has now been fetched
            fetched.add(name);
        }

        return delegate.get(name);
    }

    /**
     * Use the IteratorBuildingVisitor limit to the current docRange to parse all delayed sub trees of the query. From those delayed sub trees initialize all
     * iterators matching the target field and aggregate all partial Documents into a list
     *
     * @param name
     *            the name of the field to fetch on demand
     * @return the list of Document objects that were fetched from all delayed iterators associated with the on-demand field
     * @throws IOException
     *             if there is an issue with read/write
     */
    private List<Document> fetchOnDemand(String name) throws IOException {
        List<Document> documentList = new ArrayList<>();

        // limit the ranges to use to the current document
        iteratorBuildingVisitor.limit(docRange);

        // for each sub tree build the nested iterator
        for (JexlNode delayedNonEventNode : delayedNonEventFieldMap.get(name)) {
            // sanity check
            if (delayedNonEventNode == null) {
                throw new IllegalStateException("Delayed nonEventNode must not be null");
            }

            // reset the root
            iteratorBuildingVisitor.resetRoot();

            // construct the index iterator for this node
            delayedNonEventNode.jjtAccept(iteratorBuildingVisitor, null);
            NestedIterator<Key> delayedNodeIterator = iteratorBuildingVisitor.root();
            if (delayedNodeIterator != null) {
                // get all the leaf nodes, this is very likely (always?)
                Collection<NestedIterator<Key>> leaves = delayedNodeIterator.leaves();
                // for each leaf, see if its a match for the target field
                for (NestedIterator<Key> leaf : leaves) {
                    // init/seek the leaf
                    leaf.initialize();
                    leaf.seek(docRange, columnFamilies, inclusive);

                    // for each value off the leaf add it to the document list as long as equality accepts it
                    while (leaf.hasNext()) {
                        Key nextKey = leaf.next();
                        if (equality.partOf(docRange.getStartKey(), nextKey)) {
                            documentList.add(leaf.document());
                        }
                    }
                }
            }
        }

        return documentList;
    }

    /**
     * Add the elements fetched by this context to the main document
     *
     * @param d
     *            the main document
     */
    public void populateDocument(Document d) {
        for (String field : fetched) {
            Object obj = get(field);
            if (obj instanceof FunctionalSet<?>) {
                FunctionalSet<?> fs = (FunctionalSet<?>) obj;
                for (Object element : fs) {
                    if (element instanceof ValueTuple) {
                        d.put(field, ((ValueTuple) element).getSource());
                    }
                }
            }
        }
    }

    @Override
    public boolean has(String name) {
        return delegate.has(name);
    }

    @Override
    public void clear() {
        this.delegate.clear();
    }

    @Override
    public int size() {
        return this.delegate.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        DelayedNonEventIndexContext that = (DelayedNonEventIndexContext) o;
        // @formatter:off
        return inclusive == that.inclusive
                && Objects.equal(delegate, that.delegate)
                && Objects.equal(delayedNonEventFieldMap, that.delayedNonEventFieldMap)
                && Objects.equal(docRange, that.docRange)
                && Objects.equal(columnFamilies, that.columnFamilies)
                && Objects.equal(equality, that.equality)
                && Objects.equal(fetched, that.fetched);
        // @formatter:on
    }

    @Override
    public int hashCode() {
        // @formatter:off
        return Objects.hashCode(
                super.hashCode(),
                delegate,
                delayedNonEventFieldMap,
                docRange,
                columnFamilies,
                inclusive,
                equality,
                fetched);
        // @formatter:on
    }
}
