package datawave.query.jexl.nodes;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;
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
        assertInstance(findInstance("(_Delayed_ = true) && FOO == '1' && BAR == '2'"), ASTDelayedPredicate.class, "(FOO == '1' && BAR == '2')");
        // Test unwrapped marker and wrapped sources
        assertInstance(findInstance("(_Delayed_ = true) && (FOO == '1' && BAR == '2')"), ASTDelayedPredicate.class, "FOO == '1' && BAR == '2'");
        // Test wrapped marker and unwrapped sources
        assertInstance(findInstance("((_Delayed_ = true) && FOO == '1' && BAR == '2')"), ASTDelayedPredicate.class, "(FOO == '1' && BAR == '2')");
        // Test wrapped marker and wrapped sources
        assertInstance(findInstance("((_Delayed_ = true) && (FOO == '1' && BAR == '2'))"), ASTDelayedPredicate.class, "FOO == '1' && BAR == '2'");
    }
    
    @Test
    public void testInstance_of() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of();
        assertFalse(instance.isAnyType());
        assertNull(instance.getSource());
        
        QueryPropertyMarker.Instance secondInstance = QueryPropertyMarker.Instance.of();
        assertSame(instance, secondInstance);
        
        JexlNode source = parseJexlQuery("(FOO == 'a')");
        instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, source);
        assertEquals(ASTDelayedPredicate.class, instance.getType());
        assertEquals(source, instance.getSource());
    }
    
    @Test
    public void testInstance_isAnyType() throws ParseException {
        assertFalse(QueryPropertyMarker.Instance.of().isAnyType());
        
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, parseJexlQuery("FOO == 'a'"));
        assertTrue(instance.isAnyType());
    }
    
    @Test
    public void testInstance_isType() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, parseJexlQuery("FOO == 'a'"));
        assertTrue(instance.isType(ASTDelayedPredicate.class));
        assertFalse(instance.isType(null));
        assertFalse(instance.isType(BoundedRange.class));
        
        assertTrue(QueryPropertyMarker.Instance.of().isType(null));
        assertFalse(QueryPropertyMarker.Instance.of().isType(ASTDelayedPredicate.class));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isAnyTypeOf() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, parseJexlQuery("FOO == 'a'"));
        
        assertTrue(instance.isAnyTypeOf(ASTDelayedPredicate.class, BoundedRange.class));
        assertTrue(instance.isAnyTypeOf(Sets.newHashSet(ASTDelayedPredicate.class, BoundedRange.class)));
        
        assertFalse(instance.isAnyTypeOf());
        assertFalse(instance.isAnyTypeOf(IndexHoleMarkerJexlNode.class, BoundedRange.class));
        assertFalse(instance.isAnyTypeOf(Sets.newHashSet(IndexHoleMarkerJexlNode.class, BoundedRange.class)));
        
        assertThrows(NullPointerException.class, () -> instance.isAnyTypeOf((Collection<Class<? extends QueryPropertyMarker>>) null));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isAnyTypeExcept() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, Lists.newArrayList(parseJexlQuery("FOO == 'a'")));
        assertTrue(instance.isAnyTypeExcept(IndexHoleMarkerJexlNode.class, BoundedRange.class));
        assertTrue(instance.isAnyTypeExcept(Sets.newHashSet(IndexHoleMarkerJexlNode.class, BoundedRange.class)));
        assertTrue(instance.isAnyTypeExcept());
        
        assertFalse(instance.isAnyTypeExcept(ASTDelayedPredicate.class, BoundedRange.class));
        assertFalse(instance.isAnyTypeExcept(Sets.newHashSet(ASTDelayedPredicate.class, BoundedRange.class)));
        
        assertThrows(NullPointerException.class, () -> instance.isAnyTypeExcept((Collection<Class<? extends QueryPropertyMarker>>) null));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testInstance_isNotAnyTypeOf() throws ParseException {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, Lists.newArrayList(parseJexlQuery("FOO == 'a'")));
        
        assertTrue(instance.isNotAnyTypeOf(IndexHoleMarkerJexlNode.class, BoundedRange.class));
        assertTrue(instance.isNotAnyTypeOf(Sets.newHashSet(IndexHoleMarkerJexlNode.class, BoundedRange.class)));
        assertTrue(QueryPropertyMarker.Instance.of().isNotAnyTypeOf(ASTDelayedPredicate.class, BoundedRange.class));
        assertTrue(QueryPropertyMarker.Instance.of().isNotAnyTypeOf(Sets.newHashSet(ASTDelayedPredicate.class, BoundedRange.class)));
        assertTrue(instance.isNotAnyTypeOf());
        
        assertFalse(instance.isNotAnyTypeOf(ASTDelayedPredicate.class, BoundedRange.class));
        assertFalse(instance.isNotAnyTypeOf(Sets.newHashSet(ASTDelayedPredicate.class, BoundedRange.class)));
        
        assertThrows(NullPointerException.class, () -> instance.isNotAnyTypeOf((Collection<Class<? extends QueryPropertyMarker>>) null));
    }
    
    @Test
    public void testInstance_isDelayedPredicate() {
        assertTrue(QueryPropertyMarker.Instance.of(IndexHoleMarkerJexlNode.class, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(ASTEvaluationOnly.class, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(ExceededOrThresholdMarkerJexlNode.class, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(ExceededTermThresholdMarkerJexlNode.class, (List<JexlNode>) null).isDelayedPredicate());
        assertTrue(QueryPropertyMarker.Instance.of(ExceededTermThresholdMarkerJexlNode.class, (List<JexlNode>) null).isDelayedPredicate());
        
        assertFalse(QueryPropertyMarker.Instance.of(BoundedRange.class, (List<JexlNode>) null).isDelayedPredicate());
    }
    
    @Test
    public void testInstance_isIvarator() {
        assertTrue(QueryPropertyMarker.Instance.of(ExceededOrThresholdMarkerJexlNode.class, (List<JexlNode>) null).isIvarator());
        assertTrue(QueryPropertyMarker.Instance.of(ExceededTermThresholdMarkerJexlNode.class, (List<JexlNode>) null).isIvarator());
        assertTrue(QueryPropertyMarker.Instance.of(ExceededTermThresholdMarkerJexlNode.class, (List<JexlNode>) null).isIvarator());
        
        assertFalse(QueryPropertyMarker.Instance.of(IndexHoleMarkerJexlNode.class, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(ASTDelayedPredicate.class, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(ASTEvaluationOnly.class, (List<JexlNode>) null).isIvarator());
        assertFalse(QueryPropertyMarker.Instance.of(BoundedRange.class, (List<JexlNode>) null).isIvarator());
    }
    
    private QueryPropertyMarker.Instance findInstance(String query) throws ParseException {
        return QueryPropertyMarker.findInstance(query != null ? parseJexlQuery(query) : null);
    }
    
    private void assertEmptyInstance(QueryPropertyMarker.Instance instance) {
        assertNull(instance.getType());
        assertNull(instance.getSource());
    }
    
    private void assertInstance(QueryPropertyMarker.Instance instance, Class<? extends QueryPropertyMarker> type, String source) {
        assertEquals(type, instance.getType());
        assertEquals(source, buildQuery(instance.getSource()));
    }
}
