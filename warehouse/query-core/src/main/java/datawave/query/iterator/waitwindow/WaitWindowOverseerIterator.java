package datawave.query.iterator.waitwindow;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import datawave.query.attributes.Document;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableNestedIterator;

/*
 * The WaitWindowOverseerIterator is between the SerialIterator/PipelineIterator and the Ivarators, boolean, and other
 * Datawave iterators that execute the query. It catches any WaitWindowOverrunExceptions during initialize, seek,
 * hasNext, and next calls. When an exception is caught, a Map.Entry<Key, Document> is created that will be used for
 * the subsequent next or document call.
 */
public class WaitWindowOverseerIterator extends SeekableNestedIterator<Key> {

    private Map.Entry<Key,Document> nextFromWaitWindowOverrunException = null;

    public WaitWindowOverseerIterator(NestedIterator<Key> source, IteratorEnvironment env) {
        super(source, env);
    }

    @Override
    public void initialize() {
        // skip initialize if we've already had a WaitWindowOverrunException
        if (this.nextFromWaitWindowOverrunException == null) {
            try {
                super.initialize();
            } catch (WaitWindowOverrunException e) {
                this.nextFromWaitWindowOverrunException = new AbstractMap.SimpleEntry<>(e.getYieldKey(), WaitWindowObserver.getWaitWindowOverrunDocument());
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (WaitWindowOverrunException e) {
            this.nextFromWaitWindowOverrunException = new AbstractMap.SimpleEntry<>(e.getYieldKey(), WaitWindowObserver.getWaitWindowOverrunDocument());
        }
    }

    @Override
    public boolean hasNext() {
        if (this.nextFromWaitWindowOverrunException == null) {
            try {
                return super.hasNext();
            } catch (WaitWindowOverrunException e) {
                this.nextFromWaitWindowOverrunException = new AbstractMap.SimpleEntry<>(e.getYieldKey(), WaitWindowObserver.getWaitWindowOverrunDocument());
                return true;
            }
        } else {
            return true;
        }
    }

    @Override
    public Key next() {
        if (this.nextFromWaitWindowOverrunException == null) {
            try {
                return super.next();
            } catch (WaitWindowOverrunException e) {
                this.nextFromWaitWindowOverrunException = new AbstractMap.SimpleEntry<>(e.getYieldKey(), WaitWindowObserver.getWaitWindowOverrunDocument());
                return this.nextFromWaitWindowOverrunException.getKey();
            }
        } else {
            return this.nextFromWaitWindowOverrunException.getKey();
        }
    }

    @Override
    public Document document() {
        if (this.nextFromWaitWindowOverrunException == null) {
            return super.document();
        } else {
            Document document = this.nextFromWaitWindowOverrunException.getValue();
            this.nextFromWaitWindowOverrunException = null;
            return document;
        }
    }
}
