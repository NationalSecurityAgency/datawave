package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DocumentAggregatingIterator {

    @Test
    public void DocumentAggregatingIteratorTest(){

    }

    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }

}
