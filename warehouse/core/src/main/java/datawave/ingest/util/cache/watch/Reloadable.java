package datawave.ingest.util.cache.watch;

/**
 *
 */
public abstract class Reloadable<V> {
    public abstract boolean hasChanged();

    public abstract V reload();
}
