package datawave.query.data.parsers;

import datawave.query.data.parsers.KeyParserFactory.PARSER_TYPE;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class KeyParserFactoryTest {

    @Test
    public void testCreateFieldIndexKey() {
        KeyParser parser = KeyParserFactory.create(PARSER_TYPE.FIELD_INDEX);
        assertTrue(parser instanceof FieldIndexKey);
    }

    @Test
    public void testCreateEventKey() {
        KeyParser parser = KeyParserFactory.create(PARSER_TYPE.EVENT);
        assertTrue(parser instanceof EventKey);
    }

    @Test
    public void testCreateTermFrequencyKey() {
        KeyParser parser = KeyParserFactory.create(PARSER_TYPE.TERM_FREQUENCY);
        assertTrue(parser instanceof TermFrequencyKey);
    }

}
