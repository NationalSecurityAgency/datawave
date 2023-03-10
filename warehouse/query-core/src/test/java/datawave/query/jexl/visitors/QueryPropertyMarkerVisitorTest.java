package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.junit.Test;

import java.util.Collections;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    
    @Test
    public void testQueryPropertyMarkerTypeWithoutOverriddenLabelMethodThrowsError() {
        JexlNode node = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        Throwable throwable = assertThrows(IllegalStateException.class, () -> QueryPropertyMarkerVisitor.instanceOf(node, DoesNotOverrideLabelMethod.class));
        assertEquals("Unable to invoke label() method for datawave.query.jexl.visitors.QueryPropertyMarkerVisitorTest$DoesNotOverrideLabelMethod",
                        throwable.getMessage());
    }
    
    @Test
    public void testQueryPropertyMarkerTypeWithEmptyLabelThrowsError() {
        JexlNode node = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        Throwable throwable = assertThrows(IllegalStateException.class, () -> QueryPropertyMarkerVisitor.instanceOf(node, HasEmptyLabel.class));
        assertEquals("label() method returns null/empty label for type datawave.query.jexl.visitors.QueryPropertyMarkerVisitorTest$HasEmptyLabel",
                        throwable.getMessage());
    }
    
    @Test
    public void testInstanceOfAny() throws ParseException {
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Hole_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Delayed_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Hole_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Eval_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_List_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Term_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Value_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("((_Bounded_ = true) && (FOO > 1 && FOO < 5))")));
        assertFalse(QueryPropertyMarkerVisitor.instanceOfAny(parseJexlQuery("FOO == 'a'")));
    }
    
    @Test
    public void testInstanceOf() throws ParseException {
        JexlNode node = parseJexlQuery("((_Bounded_ = true) && (FOO > 1 && FOO < 5))");
        assertTrue(QueryPropertyMarkerVisitor.instanceOf(node, BoundedRange.class));
        assertFalse(QueryPropertyMarkerVisitor.instanceOf(node, ASTDelayedPredicate.class));
        assertTrue(QueryPropertyMarkerVisitor.instanceOf(node, Collections.singleton(BoundedRange.class)));
        assertFalse(QueryPropertyMarkerVisitor.instanceOf(node, Collections.singleton(ASTDelayedPredicate.class)));
    }
    
    @Test
    public void testInstanceOfAnyExcept() throws ParseException {
        JexlNode node = parseJexlQuery("((_Bounded_ = true) && (FOO > 1 && FOO < 5))");
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAnyExcept(node, ASTDelayedPredicate.class));
        assertFalse(QueryPropertyMarkerVisitor.instanceOfAnyExcept(node, BoundedRange.class));
        assertTrue(QueryPropertyMarkerVisitor.instanceOfAnyExcept(node, Collections.singleton(ASTDelayedPredicate.class)));
        assertFalse(QueryPropertyMarkerVisitor.instanceOfAnyExcept(node, Collections.singleton(BoundedRange.class)));
    }
    
    @Test
    public void testIsDelayedPredicate() throws ParseException {
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Hole_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Delayed_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Hole_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Eval_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_List_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Term_ = true) && (FOO == 'a'))")));
        assertTrue(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Value_ = true) && (FOO == 'a'))")));
        assertFalse(QueryPropertyMarkerVisitor.isDelayedPredicate(parseJexlQuery("((_Bounded_ = true) && (FOO > 1 && FOO < 5))")));
    }
}
