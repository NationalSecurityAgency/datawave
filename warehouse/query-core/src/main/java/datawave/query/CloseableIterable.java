package datawave.query;

import java.io.Closeable;

/**
 *
 */
public interface CloseableIterable<T> extends Iterable<T>, Closeable {

}
