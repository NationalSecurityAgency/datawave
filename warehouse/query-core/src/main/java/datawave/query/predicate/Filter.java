package datawave.query.predicate;

import org.apache.accumulo.core.data.Key;

/**
 * A filter intended to filter event data, field index data, or term frequency data for evaluation purposes, and to mark those that are to be kept in the final
 * document.
 */
public interface Filter {
    public boolean keep(Key k);
}
