package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.test.JexlNodeAssertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QueryPropertyMarkerVisitorTest {
    
    private QueryPropertyMarker.Instance instance;
    
    @Before
    public void setUp() throws Exception {
        instance = null;
    }
    
    @Test
    public void testNull() {
        instance = QueryPropertyMarkerVisitor.getInstance(null);
        assertNoType();
        assertNullSource();
    }
    
    @Test
    public void testAndNodeWithoutMarker() throws ParseException {
        givenNode("FOO == 1 && BAR == 2");
        assertNoType();
        assertNullSource();
    }
    
    @Test
    public void testAndWithNestedMarker() throws ParseException {
        givenNode("ABC == 'aaa' && ((_Eval_ = true) && (FOO == 1 && BAR == 2))");
        assertNoType();
        assertNullSource();
    }
    
    @Test
    public void testAndWithMarkersOnBothSides() throws ParseException {
        givenNode("((_Bounded_ = true) && (FOO > 1 && FOO < 2)) && ((_Eval_ = true) && (FOO == 1 && BAR == 2))");
        assertNoType();
        assertNullSource();
    }
    
    @Test
    public void testUnwrappedMarkerAndUnwrappedSources() throws ParseException {
        givenNode("(_Delayed_ = true) && FOO == 1 && BAR == 2");
        assertType(DELAYED);
        assertSources("FOO == 1", "BAR == 2");
    }
    
    @Test
    public void testUnwrappedMarkerAndWrappedSources() throws ParseException {
        givenNode("(_Delayed_ = true) && (FOO == 1 && BAR == 2)");
        assertType(DELAYED);
        assertSources("FOO == 1 && BAR == 2");
    }
    
    @Test
    public void testWrappedMarkerAndUnwrappedSources() throws ParseException {
        givenNode("((_Delayed_ = true) && FOO == 1 && BAR == 2)");
        assertType(DELAYED);
        assertSources("FOO == 1", "BAR == 2");
    }
    
    @Test
    public void testUnwrappedNestedSources() throws ParseException {
        givenNode("((_Delayed_ = true) && FOO == 1 && (BAR == 2 && BAT == 4))");
        assertType(DELAYED);
        assertSources("FOO == 1", "BAR == 2 && BAT == 4");
    }
    
    @Test
    public void testWrappedMarkerAndWrappedSources() throws ParseException {
        givenNode("((_Eval_ = true) && (FOO == 1 && BAR == 2))");
        assertType(EVALUATION_ONLY);
        assertSources("FOO == 1 && BAR == 2");
    }
    
    private void givenNode(String query) throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        PrintingVisitor.printQuery(node);
        instance = QueryPropertyMarkerVisitor.getInstance(node);
    }
    
    private void assertNoType() {
        assertFalse(instance.isAnyType());
    }
    
    private void assertType(QueryPropertyMarker.MarkerType type) {
        assertTrue(instance.isType(type));
    }
    
    private void assertNullSource() {
        assertNull(instance.getSource());
    }
    
    private void assertSources(String... sources) {
        assertThat(instance.getSources()).asStrings().containsExactlyInAnyOrder(sources);
    }
}
