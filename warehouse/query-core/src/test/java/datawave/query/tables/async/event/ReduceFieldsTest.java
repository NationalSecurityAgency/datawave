package datawave.query.tables.async.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;

public class ReduceFieldsTest {

    @Test
    public void testGetQueryFields() throws ParseException {
        Set<String> expectedFields = Sets.newHashSet("FOO", "FOO2");

        String query = "FOO == 'bar' && FOO2 == 'baz'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        Set<String> queryFields = ReduceFields.getQueryFields(script);
        assertEquals(expectedFields, queryFields);

        // and assert list ivarator
        query = "FOO == 'bar' &&  ((_List_ = true) && ((id = 'some-bogus-id') && (field = 'FOO2')))";
        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        queryFields = ReduceFields.getQueryFields(script);
        assertEquals(expectedFields, queryFields);
    }

    @Test
    public void testIntersectFields() {
        Set<String> queryFields = Sets.newHashSet("F1", "F2", "F3");
        Set<String> optionFields = Sets.newHashSet("F2", "F3", "F4");
        Set<String> expectedFields = Sets.newHashSet("F2", "F3");

        Set<String> intersectedFields = ReduceFields.intersectFields(queryFields, optionFields);
        assertEquals(expectedFields, intersectedFields);
    }

    @Test
    public void testIntersectFieldsWithOption() {
        Set<String> queryFields = Sets.newHashSet("F1", "F2", "F3");
        String option = "F2,F3,F4";

        String intersected = ReduceFields.intersectFields(queryFields, option);
        assertEquals("F2,F3", intersected);
    }

    @Test
    public void testIntersectFieldForNonExistentOption() {
        Set<String> queryFields = Sets.newHashSet("F1", "F2", "F3");
        String option = "F4,F5,F6";

        String intersected = ReduceFields.intersectFields(queryFields, option);
        assertNull(intersected);
    }

    @Test
    public void testReduceFieldsForOption() {
        String indexedFields = "F1,F2,F3,F4";
        Set<String> queryFields = Sets.newHashSet("F2", "F3");

        IteratorSetting setting = new IteratorSetting(20, "IterForTests", "Iterator.class");
        setting.addOption(QueryOptions.INDEXED_FIELDS, indexedFields);

        // assert initial state
        assertEquals(indexedFields, setting.getOptions().get(QueryOptions.INDEXED_FIELDS));

        // assert reduced state
        ReduceFields.reduceFieldsForOption(QueryOptions.INDEXED_FIELDS, queryFields, setting);
        assertEquals("F2,F3", setting.getOptions().get(QueryOptions.INDEXED_FIELDS));
    }

    @Test
    public void testReduceFieldsForOptionNonExistentOption() {
        String indexedFields = "F1,F2,F3,F4";
        Set<String> queryFields = Sets.newHashSet("F2", "F3");

        IteratorSetting setting = new IteratorSetting(20, "IterForTests", "Iterator.class");
        setting.addOption(QueryOptions.INDEXED_FIELDS, indexedFields);

        // assert initial state
        assertEquals(indexedFields, setting.getOptions().get(QueryOptions.INDEXED_FIELDS));

        // assert no change to indexed fields after reducing composite fields
        ReduceFields.reduceFieldsForOption(QueryOptions.COMPOSITE_FIELDS, queryFields, setting);
        assertEquals(indexedFields, setting.getOptions().get(QueryOptions.INDEXED_FIELDS));
    }
}
