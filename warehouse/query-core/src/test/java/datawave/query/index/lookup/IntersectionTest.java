package datawave.query.index.lookup;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Collections;

public class IntersectionTest {
    
    @Test
    public void testEmptyExceededValueThreshold() throws ParseException {
        List<? extends IndexStream> iterable = Arrays.asList(ScannerStream.exceededValueThreshold(Collections.emptyIterator(),
                        JexlASTHelper.parseJexlQuery("THIS_FIELD == 20")));
        
        Intersection intersection = new Intersection(iterable, null);
        
        Assert.assertEquals(IndexStream.StreamContext.ABSENT, intersection.context());
    }
}
