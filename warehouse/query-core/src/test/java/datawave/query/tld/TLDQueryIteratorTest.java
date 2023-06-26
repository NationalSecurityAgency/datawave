package datawave.query.tld;

import datawave.query.function.Equality;
import datawave.query.function.RangeProvider;
import datawave.query.function.TLDEquality;
import datawave.query.function.TLDRangeProvider;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.postprocessing.tf.TermFrequencyConfig;
import datawave.query.util.TypeMetadata;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TLDQueryIteratorTest {

    @Test
    public void testBuildTfFunction() throws ParseException {
        TermFrequencyConfig config = new TermFrequencyConfig();
        config.setTypeMetadata(new TypeMetadata());
        config.setScript(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'"));
        assertFalse("tld flag should be false by default", config.isTld());

        TLDQueryIterator tldIter = new TLDQueryIterator();
        tldIter.buildTfFunction(config);
        assertTrue("TLDQueryIterator sets the isTld flag during build", config.isTld());
    }

    @Test
    public void testGetRangeProvider() {
        TLDQueryIterator iterator = new TLDQueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(TLDRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }

    @Test
    public void testGetEquality() {
        TLDQueryIterator iterator = new TLDQueryIterator();
        Equality equality = iterator.getEquality();
        assertEquals(TLDEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }
}
