package datawave.query.jexl.nodes;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;
import static datawave.query.jexl.visitors.JexlStringBuildingVisitor.buildQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QueryPropertyMarkerTest {
    
    @Test
    public void testFindInstance() throws ParseException {
        assertEmptyInstance(findInstance(null)); // Test null input
        assertEmptyInstance(findInstance("FOO == '1' && BAR == '2'")); // Test node without marker
        assertEmptyInstance(findInstance("ABC == 'aaa' && ((_EvalOnly_ = true) && (FOO == '1' && BAR == '2'))")); // Test node with nested marker child
        // Test and node with markers on both sides
        assertEmptyInstance(findInstance("((_Bounded_ = true) && (FOO > '1' && FOO < '2')) && ((_EvalOnly_ = true) && (FOO == '1' && BAR == '2'))"));
        
        // Test unwrapped marker and unwrapped sources
        assertInstance(findInstance("(_Delayed_ = true) && FOO == '1' && BAR == '2'"), DELAYED, "(FOO == '1' && BAR == '2')");
        // Test unwrapped marker and wrapped sources
        assertInstance(findInstance("(_Delayed_ = true) && (FOO == '1' && BAR == '2')"), DELAYED, "FOO == '1' && BAR == '2'");
        // Test wrapped marker and unwrapped sources
        assertInstance(findInstance("((_Delayed_ = true) && FOO == '1' && BAR == '2')"), DELAYED, "(FOO == '1' && BAR == '2')");
        // Test wrapped marker and wrapped sources
        assertInstance(findInstance("((_Delayed_ = true) && (FOO == '1' && BAR == '2'))"), DELAYED, "FOO == '1' && BAR == '2'");
    }
    
    @Test
    public void testInstance_of() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of();
        assertFalse(instance.isAnyType());
        assertNull(instance.getSource());
        
        QueryPropertyMarker.Instance secondInstance = QueryPropertyMarker.Instance.of();
        assertSame(instance, secondInstance);
        
        JexlNode source = parseJexlQuery("(FOO == 'a')");
        instance = QueryPropertyMarker.Instance.of(DELAYED, source);
        assertEquals(DELAYED, instance.getType());
        assertEquals(source, instance.getSource());
    }
    
    @Test
    public void testInstance_isAnyType() throws ParseException {
        assertFalse(QueryPropertyMarker.Instance.of().isAnyType());
        
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(DELAYED, parseJexlQuery("FOO == 'a'"));
        assertTrue(instance.isAnyType());
    }
    
    @Test
    public void testInstance_isType() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(DELAYED, parseJexlQuery("FOO == 'a'"));
        assertTrue(instance.isType(DELAYED));
        assertFalse(instance.isType(null));
        assertFalse(instance.isType(BOUNDED_RANGE));
        
        assertTrue(QueryPropertyMarker.Instance.of().isType(null));
        assertFalse(QueryPropertyMarker.Instance.of().isType(DELAYED));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isAnyTypeOf() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(DELAYED, parseJexlQuery("FOO == 'a'"));
        
        assertTrue(instance.isAnyTypeOf(DELAYED, BOUNDED_RANGE));
        assertTrue(instance.isAnyTypeOf(Sets.newHashSet(DELAYED, BOUNDED_RANGE)));
        
        assertFalse(instance.isAnyTypeOf());
        assertFalse(instance.isAnyTypeOf(INDEX_HOLE, BOUNDED_RANGE));
        assertFalse(instance.isAnyTypeOf(Sets.newHashSet(INDEX_HOLE, BOUNDED_RANGE)));
        
        assertThrows(NullPointerException.class, () -> instance.isAnyTypeOf((Collection<QueryPropertyMarker.MarkerType>) null));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isAnyTypeExcept() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(DELAYED, Lists.newArrayList(parseJexlQuery("FOO == 'a'")));
        assertTrue(instance.isAnyTypeExcept(INDEX_HOLE, BOUNDED_RANGE));
        assertTrue(instance.isAnyTypeExcept(Sets.newHashSet(INDEX_HOLE, BOUNDED_RANGE)));
        assertTrue(instance.isAnyTypeExcept());
        
        assertFalse(instance.isAnyTypeExcept(DELAYED, BOUNDED_RANGE));
        assertFalse(instance.isAnyTypeExcept(Sets.newHashSet(DELAYED, BOUNDED_RANGE)));
        
        assertThrows(NullPointerException.class, () -> instance.isAnyTypeExcept((Collection<QueryPropertyMarker.MarkerType>) null));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isNotAnyTypeOf() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(DELAYED, Lists.newArrayList(parseJexlQuery("FOO == 'a'")));
        
        assertTrue(instance.isNotAnyTypeOf(INDEX_HOLE, BOUNDED_RANGE));
        assertTrue(instance.isNotAnyTypeOf(Sets.newHashSet(INDEX_HOLE, BOUNDED_RANGE)));
        assertTrue(QueryPropertyMarker.Instance.of().isNotAnyTypeOf(DELAYED, BOUNDED_RANGE));
        assertTrue(QueryPropertyMarker.Instance.of().isNotAnyTypeOf(Sets.newHashSet(DELAYED, BOUNDED_RANGE)));
        assertTrue(instance.isNotAnyTypeOf());
        
        assertFalse(instance.isNotAnyTypeOf(DELAYED, BOUNDED_RANGE));
        assertFalse(instance.isNotAnyTypeOf(Sets.newHashSet(DELAYED, BOUNDED_RANGE)));
        
        assertThrows(NullPointerException.class, () -> instance.isNotAnyTypeOf((Collection<QueryPropertyMarker.MarkerType>) null));
    }
    
    @Test
    public void testInstance_isDelayedPredicate() {
        assertTrue(QueryPropertyMarker.Instance.of(INDEX_HOLE, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(DELAYED, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(EVALUATION_ONLY, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_OR, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_TERM, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_TERM, (List<JexlNode>) null).isDelayedPredicate());
        
        assertFalse(QueryPropertyMarker.Instance.of(BOUNDED_RANGE, (List<JexlNode>) null).isDelayedPredicate());
    }
    
    @Test
    public void testInstance_isIvarator() {
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_OR, (List<JexlNode>) null).isIvarator());
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_TERM, (List<JexlNode>) null).isIvarator());
        assertTrue(QueryPropertyMarker.Instance.of(EXCEEDED_TERM, (List<JexlNode>) null).isIvarator());
        
        assertFalse(QueryPropertyMarker.Instance.of(INDEX_HOLE, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(DELAYED, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(EVALUATION_ONLY, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(BOUNDED_RANGE, (List<JexlNode>) null).isIvarator());
    }
    
    @Test
    public void testNoDoubleMarks_ExceededOr() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_List_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, EXCEEDED_OR);
    }
    
    @Test
    public void testNoDoubleMarks_ExceededTerm() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Term_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, EXCEEDED_TERM);
    }
    
    @Test
    public void testNoDoubleMarks_ExceededValue() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Value_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, EXCEEDED_VALUE);
    }
    
    @Test
    public void testNoDoubleMarks_IndexHoleMarker() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Hole_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, INDEX_HOLE);
    }
    
    @Test
    public void testNoDoubleMarks_DelayedPredicate() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Delayed_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, DELAYED);
    }
    
    @Test
    public void testNoDoubleMarks_EvaluationOnly() {
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Eval_ = true) && (FOO =~ 'ba.*'))";
        testApplyMarkerMultipleTimes(source, expected, EVALUATION_ONLY);
    }
    
    @Test
    public void testNoDoubleMarks_BoundedRange() {
        JexlNode left = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), "FOO", "3");
        JexlNode right = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), "FOO", "7");
        JexlNode source = JexlNodeFactory.createAndNode(Arrays.asList(left, right));
        String expected = "((_Bounded_ = true) && (FOO >= '3' && FOO <= '7'))";
        testApplyMarkerMultipleTimes(source, expected, BOUNDED_RANGE);
    }
    
    private void testApplyMarkerMultipleTimes(JexlNode source, String expected, QueryPropertyMarker.MarkerType type) {
        
        // assert first marker apply
        JexlNode marked = QueryPropertyMarker.create(source, type);
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(marked));
        
        // apply marker several more times and assert nothing changes
        for (int i = 0; i < 5; i++) {
            marked = QueryPropertyMarker.create(marked, type);
        }
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(marked));
    }
    
    @Test
    public void testApplyMultipleMarkers() {
        // mark a regex as value exceeded
        JexlNode source = JexlNodeFactory.buildERNode("FOO", "ba.*");
        String expected = "((_Value_ = true) && (FOO =~ 'ba.*'))";
        JexlNode marked = QueryPropertyMarker.create(source, EXCEEDED_VALUE);
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(marked));
        
        // then delay it to skip global index lookup
        expected = "((_Delayed_ = true) && ((_Value_ = true) && (FOO =~ 'ba.*')))";
        JexlNode delayed = QueryPropertyMarker.create(marked, DELAYED);
        assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(delayed));
    }
    
    private QueryPropertyMarker.Instance findInstance(String query) throws ParseException {
        return QueryPropertyMarker.findInstance(query != null ? parseJexlQuery(query) : null);
    }
    
    private void assertEmptyInstance(QueryPropertyMarker.Instance instance) {
        assertNull(instance.getType());
        assertNull(instance.getSource());
    }
    
    private void assertInstance(QueryPropertyMarker.Instance instance, QueryPropertyMarker.MarkerType type, String source) {
        assertEquals(type, instance.getType());
        assertEquals(source, buildQuery(instance.getSource()));
    }
}
