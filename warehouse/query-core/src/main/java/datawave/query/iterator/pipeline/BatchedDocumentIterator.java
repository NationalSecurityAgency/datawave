package datawave.query.iterator.pipeline;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedSet;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import com.google.common.collect.Sets;

public class BatchedDocumentIterator implements Iterator<Entry<Key,Document>> {

    SortedSet<Entry<Key,Document>> sortedResponses = null;
    Iterator<Entry<Key,Document>> subIter = null;
    private PipelineIterator parent;

    public BatchedDocumentIterator(PipelineIterator parent) {
        this.parent = parent;
        sortedResponses = Sets.newTreeSet(Comparator.comparing(Entry::getKey));
    }

    @Override
    public boolean hasNext() {
        boolean hasResults = false;
        while (parent.hasNext()) {
            sortedResponses.add(parent.next());
            hasResults = true;
        }
        if (hasResults == true)
            subIter = sortedResponses.iterator();

        if (subIter == null) {
            return false;
        }

        return subIter.hasNext();
    }

    @Override
    public Entry<Key,Document> next() {
        return subIter.next();
    }

    @Override
    public void remove() {

    }

}
