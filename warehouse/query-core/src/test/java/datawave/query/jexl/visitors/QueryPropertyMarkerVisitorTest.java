package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

import static datawave.test.JexlNodeAssertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QueryPropertyMarkerVisitorTest {
    
    private static class DoesNotOverrideLabelMethod extends QueryPropertyMarker {
        
        public DoesNotOverrideLabelMethod() {
            super();
        }
        
        @Override
        public String getLabel() {
            return null;
        }
    }
    
    private static class HasEmptyLabel extends QueryPropertyMarker {
        
        @SuppressWarnings("unused")
        public static String label() {
            return "";
        }
        
        public HasEmptyLabel() {
            super();
        }
        
        @Override
        public String getLabel() {
            return "";
        }
    }
    
    private static class HasDuplicateLabel extends QueryPropertyMarker {
        
        @SuppressWarnings("unused")
        public static String label() {
            return ASTDelayedPredicate.label();
        }
        
        public HasDuplicateLabel() {
            super();
        }
        
        @Override
        public String getLabel() {
            return ASTDelayedPredicate.label();
        }
    }
    
    private static class HasValidLabel extends QueryPropertyMarker {
        
        @SuppressWarnings("unused")
        public static String label() {
            return "_valid_";
        }
        
        public HasValidLabel() {
            super();
        }
        
        @Override
        public String getLabel() {
            return "_valid_";
        }
    }
    
    private QueryPropertyMarker.Instance instance;
    
    @Before
    public void setUp() throws Exception {
        instance = null;
    }
    
    @Test
    public void testRegisteringNullMarker() {
        Throwable throwable = assertThrows(NullPointerException.class, () -> QueryPropertyMarkerVisitor.registerMarker(null));
        assertEquals("Marker class must not be null", throwable.getMessage());
    }
    
    @Test
    public void testRegisteringMarkerWithoutOverriddenLabelMethod() {
        Throwable throwable = assertThrows(NoSuchMethodException.class, () -> QueryPropertyMarkerVisitor.registerMarker(DoesNotOverrideLabelMethod.class));
        assertEquals("datawave.query.jexl.visitors.QueryPropertyMarkerVisitorTest$DoesNotOverrideLabelMethod.label()", throwable.getMessage());
    }
    
    @Test
    public void testRegisteringMarkerWithEmptyLabel() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> QueryPropertyMarkerVisitor.registerMarker(HasEmptyLabel.class));
        assertEquals("label() method must return a unique, non-empty label for type datawave.query.jexl.visitors.QueryPropertyMarkerVisitorTest$HasEmptyLabel",
                        throwable.getMessage());
    }
    
    @Test
    public void testRegisteringMarkerWithDuplicateLabel() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> QueryPropertyMarkerVisitor.registerMarker(HasDuplicateLabel.class));
        assertEquals("datawave.query.jexl.visitors.QueryPropertyMarkerVisitorTest$HasDuplicateLabel has the same label as org.apache.commons.jexl2.parser.ASTDelayedPredicate, labels must be unique",
                        throwable.getMessage());
    }
    
    @Test
    public void testRegisteringAlreadyRegisteredMarker() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        assertFalse(QueryPropertyMarkerVisitor.registerMarker(ASTDelayedPredicate.class));
    }
    
    @Test
    public void testRegisteringMarkerNewValidLabel() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, ParseException {
        assertTrue(QueryPropertyMarkerVisitor.registerMarker(HasValidLabel.class));
        
        givenNode("((_valid_ = true ) && FOO == 1)");
        assertType(HasValidLabel.class);
        assertSources("FOO == 1");
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
        assertType(ASTDelayedPredicate.class);
        assertSources("FOO == 1", "BAR == 2");
    }
    
    @Test
    public void testUnwrappedMarkerAndWrappedSources() throws ParseException {
        givenNode("(_Delayed_ = true) && (FOO == 1 && BAR == 2)");
        assertType(ASTDelayedPredicate.class);
        assertSources("FOO == 1 && BAR == 2");
    }
    
    @Test
    public void testWrappedMarkerAndUnwrappedSources() throws ParseException {
        givenNode("((_Delayed_ = true) && FOO == 1 && BAR == 2)");
        assertType(ASTDelayedPredicate.class);
        assertSources("FOO == 1", "BAR == 2");
    }
    
    @Test
    public void testUnwrappedNestedSources() throws ParseException {
        givenNode("((_Delayed_ = true) && FOO == 1 && (BAR == 2 && BAT == 4))");
        assertType(ASTDelayedPredicate.class);
        assertSources("FOO == 1", "BAR == 2 && BAT == 4");
    }
    
    @Test
    public void testWrappedMarkerAndWrappedSources() throws ParseException {
        givenNode("((_Eval_ = true) && (FOO == 1 && BAR == 2))");
        assertType(ASTEvaluationOnly.class);
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
    
    private void assertType(Class<? extends QueryPropertyMarker> type) {
        assertTrue(instance.isType(type));
    }
    
    private void assertNullSource() {
        assertNull(instance.getSource());
    }
    
    private void assertSources(String... sources) {
        assertThat(instance.getSources()).asStrings().containsExactlyInAnyOrder(sources);
    }
}
