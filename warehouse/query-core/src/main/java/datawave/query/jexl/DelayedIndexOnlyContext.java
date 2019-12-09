package datawave.query.jexl;

import datawave.query.attributes.Document;
import datawave.query.function.Equality;
import datawave.query.iterator.FieldedIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableIterator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;

import javax.print.Doc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Responsible for retrieving delayed fields for the specified docRange on demand and merging it with any values already in the delegate. Use the
 * IteratorBuildingVisitor on each delayed sub tree to generate iterators over the docRange.
 */
public class DelayedIndexOnlyContext extends DatawaveJexlContext {
    private DatawaveJexlContext delegate;
    private Set<String> delayedFields;
    private IteratorBuildingVisitor iteratorBuildingVisitor;
    private Set<JexlNode> delayedSubTrees;
    private Range docRange;
    private Equality equality;
    
    /**
     * track which fields have been fetched already
     */
    private Set<String> fetched;
    
    public DelayedIndexOnlyContext(DatawaveJexlContext delegate, IteratorBuildingVisitor iteratorBuildingVisitor, Set<JexlNode> delayedSubTrees,
                    Set<String> delayedFields, Range docRange, Equality equality) {
        this.delegate = delegate;
        this.iteratorBuildingVisitor = iteratorBuildingVisitor;
        this.delayedSubTrees = delayedSubTrees;
        this.delayedFields = delayedFields;
        this.docRange = docRange;
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
        if ((null != name) && delayedFields.contains(name) && !fetched.contains(name)) {
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
     */
    private List<Document> fetchOnDemand(String name) throws IOException {
        List<Document> documentList = new ArrayList<>();
        
        // limit the ranges to use to the current document
        iteratorBuildingVisitor.limit(docRange);
        
        // for each sub tree build the nested iterator
        for (JexlNode subTree : delayedSubTrees) {
            // short circuit trees that have no identifiers
            if (JexlASTHelper.getIdentifiers(subTree).size() == 0) {
                continue;
            }
            
            // construct all index iterators for this sub tree
            subTree.jjtAccept(iteratorBuildingVisitor, null);
            NestedIterator<Key> subTreeNestedIterator = iteratorBuildingVisitor.root();
            if (subTreeNestedIterator != null) {
                // get all the leaf nodes, discarding the rest
                Collection<NestedIterator<Key>> subTreeLeaves = subTreeNestedIterator.leaves();
                // for each leaf, see if its a match for the target field
                for (NestedIterator<Key> leaf : subTreeLeaves) {
                    // grab the field off the leaf and make sure it matches
                    if (leaf instanceof FieldedIterator && name.equals(((FieldedIterator) leaf).getField())) {
                        // init/seek the leaf
                        leaf.initialize();
                        if (leaf instanceof SeekableIterator) {
                            ((SeekableIterator) leaf).seek(docRange, null, false);
                        }
                        
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
        }
        
        return documentList;
    }
}
