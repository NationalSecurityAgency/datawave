package datawave.query.rewrite.function;

import org.apache.accumulo.core.data.Key;

/**
 * A key equality implementation that compares to the root pointers of two doc Ids together.
 * 
 * For example, two IDs `h1.h2.h3.a.b.c.d` and `h1.h2.h3.e.f` would be considered equal by this check.
 * 
 *
 */
public class AncestorEquality implements Equality {
    
    @Override
    public boolean partOf(Key docKey, Key test) {
        return docKey.getColumnFamily().toString().startsWith(test.getColumnFamily().toString());
    }
    
}
