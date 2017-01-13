package nsa.datawave.query.rewrite;

import java.io.Closeable;

/**
 * 
 */
public interface CloseableIterable<T> extends Iterable<T>, Closeable {
    
}
