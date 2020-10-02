package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexIteratorTest {

    @Test
    public void IndexIteratorTest() {

        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();

        childIncludes.add(getItr(Lists.newArrayList("b", "c"), false));
        childExcludes.add(getItr(Lists.newArrayList("a", "b"), false));

        //IndexIterator childOr = new  IndexIterator(new IndexIterator.Builder());

        Set<NestedIterator<String>> includes = new HashSet<>();
        //includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));

        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();

        Assert.assertFalse(iterator.isContextRequired());
        //Assert.assertTrue(childOr.isContextRequired());

        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertFalse(iterator.hasNext());

    }

    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
