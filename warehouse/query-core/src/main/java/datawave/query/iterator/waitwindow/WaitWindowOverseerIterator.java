package datawave.query.iterator.waitwindow;

import java.io.IOException;
import java.util.Collection;

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

    private WaitWindowOverrunException waitWindowOverrunException = null;

    public WaitWindowOverseerIterator(NestedIterator<Key> source, IteratorEnvironment env) {
        super(source, env);
    }

    @Override
    public void initialize() {
        // skip initialize if we've already had a WaitWindowOverrunException
        if (this.waitWindowOverrunException == null) {
            try {
                super.initialize();
            } catch (WaitWindowOverrunException e) {
                this.waitWindowOverrunException = e;
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (WaitWindowOverrunException e) {
            this.waitWindowOverrunException = e;
        }
    }

    @Override
    public boolean hasNext() {
        if (this.waitWindowOverrunException == null) {
            try {
                return super.hasNext();
            } catch (WaitWindowOverrunException e) {
                this.waitWindowOverrunException = e;
                return true;
            }
        } else {
            return true;
        }
    }

    @Override
    public Key next() {
        if (this.waitWindowOverrunException == null) {
            try {
                return super.next();
            } catch (WaitWindowOverrunException e) {
                this.waitWindowOverrunException = e;
                throw waitWindowOverrunException;
            }
        } else {
            throw waitWindowOverrunException;
        }
    }

    @Override
    public Document document() {
        if (this.waitWindowOverrunException == null) {
            return super.document();
        } else {
            this.waitWindowOverrunException = null;
            return WaitWindowObserver.getWaitWindowOverrunDocument();
        }
    }
}
