package datawave.query.ancestor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import datawave.query.function.AncestorEquality;
import datawave.query.function.AncestorRangeProvider;
import datawave.query.function.Equality;
import datawave.query.function.RangeProvider;

public class AncestorQueryIteratorTest {

    @Test
    public void testGetRangeProvider() {
        AncestorQueryIterator iterator = new AncestorQueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(AncestorRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }

    @Test
    public void testGetEquality() {
        AncestorQueryIterator iterator = new AncestorQueryIterator();
        Equality equality = iterator.getEquality();
        assertEquals(AncestorEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }

}
