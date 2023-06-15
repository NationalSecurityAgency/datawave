package datawave.query.iterator;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;
import com.google.common.base.Functions;

import datawave.query.attributes.Document;

/**
 *
 */
public class AccumuloFieldIndexIterable extends AccumuloTreeIterable<Key,Key> {

    public AccumuloFieldIndexIterable() {
        tree = null;
    }

    private AccumuloFieldIndexIterable(NestedIterator<Key> tree, Function<Entry<Key,Document>,Entry<Key,Document>> func) {
        this(tree);
    }

    public AccumuloFieldIndexIterable(NestedIterator<Key> tree) {
        this.tree = tree;
        this.func = Functions.identity();
    }

}
