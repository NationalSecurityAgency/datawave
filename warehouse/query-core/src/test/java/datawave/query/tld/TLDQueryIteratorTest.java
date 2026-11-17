package datawave.query.tld;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.function.Equality;
import datawave.query.function.RangeProvider;
import datawave.query.function.TLDEquality;
import datawave.query.function.TLDRangeProvider;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.postprocessing.tf.TermFrequencyConfig;
import datawave.query.util.TypeMetadata;

public class TLDQueryIteratorTest {

    @Test
    public void testBuildTfFunction() throws ParseException {
        TermFrequencyConfig config = new TermFrequencyConfig();
        config.setTypeMetadata(new TypeMetadata());
        config.setScript(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'"));
        assertFalse(config.isTld(), "tld flag should be false by default");

        TLDQueryIterator tldIter = new TLDQueryIterator();
        tldIter.buildTfFunction(config);
        assertTrue(config.isTld(), "TLDQueryIterator sets the isTld flag during build");
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
