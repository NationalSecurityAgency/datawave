package datawave.query.iterator.pipeline;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.iterator.DocumentSpecificNestedIterator;
import datawave.query.iterator.NestedQueryIterator;

/**
 * A pipeline that can be executed as a runnable
 */
public class Pipeline implements Runnable {

    private static final Logger log = Logger.getLogger(Pipeline.class);
    /**
     * A source list for which the iterator will automatically reset to the beginning upon comodification. This allows us to have an iterator that will always
     * return results as long as elements are added to the list.
     */
    private DocumentSpecificNestedIterator documentSpecificSource = new DocumentSpecificNestedIterator(null);

    // the result
    private Entry<Key,Document> result = null;
    // exception
    private RuntimeException exception = null;
    // the pipeline
    private Iterator<Entry<Key,Document>> iterator = null;
    private AtomicBoolean running = new AtomicBoolean(false);

    public Pipeline() {}

    public void setSourceIterator(Iterator<Entry<Key,Document>> sourceIter) {
        this.iterator = sourceIter;
    }

    public NestedQueryIterator<Key> getDocumentSpecificSource() {
        return documentSpecificSource;
    }

    public void setSource(Map.Entry<Key,Document> documentKey) {
        this.documentSpecificSource.setDocumentKey(documentKey);
    }

    public Map.Entry<Key,Document> getSource() {
        return this.documentSpecificSource.getDocumentKey();
    }

    public void clear() {
        this.exception = null;
        this.result = null;
        this.documentSpecificSource.setDocumentKey(null);
    }

    public Entry<Key,Document> getResult() {
        if (exception == null) {
            return result;
        } else {
            throw exception;
        }
    }

    public void waitUntilComplete() {
        if (running.get()) {
            synchronized (running) {
                while (running.get()) {
                    try {
                        running.wait();
                    } catch (InterruptedException e) {

                    }
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            running.set(true);
            if (iterator.hasNext()) {
                result = iterator.next();
            } else {
                result = null;
            }

            if (log.isTraceEnabled()) {
                log.trace("next() returned " + result);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                exception = (RuntimeException) e;
            } else {
                exception = new RuntimeException(e);
            }
        } finally {
            synchronized (running) {
                running.set(false);
                running.notify();
            }
        }
    }
}
