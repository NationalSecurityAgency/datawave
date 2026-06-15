package datawave.marking;

import static datawave.marking.AccessExpressionMarkings.ACCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class AccessExpressionUtilTest {

    // -- toColumnVisibility --

    @Test
    public void testToColumnVisibilityNull() {
        assertNull(AccessExpressionUtil.toColumnVisibility(null));
    }

    @Test
    public void testToColumnVisibilityEmpty() {
        AccessExpression ae = ACCESS.newExpression("");
        ColumnVisibility cv = AccessExpressionUtil.toColumnVisibility(ae);
        assertEquals(0, cv.getExpression().length);
    }

    @Test
    public void testToColumnVisibilitySimple() {
        AccessExpression ae = ACCESS.newExpression("A&B");
        ColumnVisibility cv = AccessExpressionUtil.toColumnVisibility(ae);
        assertEquals("A&B", new String(cv.getExpression()));
    }

    // -- toAccessExpression(ColumnVisibility) --

    @Test
    public void testToAccessExpressionFromColumnVisibilityNull() {
        assertNull(AccessExpressionUtil.toAccessExpression((ColumnVisibility) null));
    }

    @Test
    public void testToAccessExpressionFromColumnVisibilityEmpty() {
        ColumnVisibility cv = new ColumnVisibility();
        AccessExpression ae = AccessExpressionUtil.toAccessExpression(cv);
        assertEquals("", ae.getExpression());
    }

    @Test
    public void testToAccessExpressionFromColumnVisibility() {
        ColumnVisibility cv = new ColumnVisibility("A|B");
        AccessExpression ae = AccessExpressionUtil.toAccessExpression(cv);
        assertEquals("A|B", ae.getExpression());
    }

    // -- toAccessExpression(String) --

    @Test
    public void testToAccessExpressionFromStringNull() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression((String) null);
        assertEquals("", ae.getExpression());
    }

    @Test
    public void testToAccessExpressionFromStringEmpty() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression("");
        assertEquals("", ae.getExpression());
    }

    @Test
    public void testToAccessExpressionFromString() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression("X&Y");
        assertEquals("X&Y", ae.getExpression());
    }

    // -- toAccessExpression(byte[]) --

    @Test
    public void testToAccessExpressionFromBytesNull() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression((byte[]) null);
        assertEquals("", ae.getExpression());
    }

    @Test
    public void testToAccessExpressionFromBytesEmpty() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression(new byte[0]);
        assertEquals("", ae.getExpression());
    }

    @Test
    public void testToAccessExpressionFromBytes() {
        AccessExpression ae = AccessExpressionUtil.toAccessExpression("C&D".getBytes());
        assertEquals("C&D", ae.getExpression());
    }

    // -- normalize --

    @Test
    public void testNormalizeEmpty() {
        AccessExpression ae = ACCESS.newExpression("");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("", normalized.getExpression());
    }

    @Test
    public void testNormalizeSortsTerms() {
        AccessExpression ae = ACCESS.newExpression("Z&Y&X");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("X&Y&Z", normalized.getExpression());
    }

    @Test
    public void testNormalizeDeduplicates() {
        AccessExpression ae = ACCESS.newExpression("A&B&A");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("A&B", normalized.getExpression());
    }

    @Test
    public void testNormalizeFlattensNestedAnds() {
        AccessExpression ae = ACCESS.newExpression("A&(B&C)");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("A&B&C", normalized.getExpression());
    }

    @Test
    public void testNormalizeSortsOrSubExpressions() {
        AccessExpression ae = ACCESS.newExpression("(Z&Y)|(C&B)");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("(B&C)|(Y&Z)", normalized.getExpression());
    }

    @Test
    public void testNormalizeRemovesUnnecessaryQuotes() {
        AccessExpression ae = ACCESS.newExpression("\"ABC\"&\"XYZ\"");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("ABC&XYZ", normalized.getExpression());
    }

    @Test
    public void testNormalizeRemovesUnnecessaryParentheses() {
        AccessExpression ae = ACCESS.newExpression("(((ABC)|(XYZ)))");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("ABC|XYZ", normalized.getExpression());
    }

    @Test
    public void testNormalizeSingleAuth() {
        AccessExpression ae = ACCESS.newExpression("ALPHA");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("ALPHA", normalized.getExpression());
    }

    @Test
    public void testNormalizeComplex1() {
        AccessExpression ae = ACCESS.newExpression("(A&A)|(B&B)|(A&A&A)");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("A|B", normalized.getExpression());
    }

    @Test
    public void testNormalizeComplex2() {
        AccessExpression ae = ACCESS.newExpression("(Y|B|Y)&(Z|A|Z)");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("(A|Z)&(B|Y)", normalized.getExpression());
    }

    @Test
    public void testNormalizeAlreadyOptimal() {
        AccessExpression ae = ACCESS.newExpression("(A|B|C)&(B|C|D)");
        AccessExpression normalized = AccessExpressionUtil.normalize(ae);
        assertEquals("(A|B|C)&(B|C|D)", normalized.getExpression());
    }

    // -- emptyExpression / getAccess --

    @Test
    public void testEmptyExpression() {
        assertEquals("", AccessExpressionUtil.emptyExpression().getExpression());
    }

    @Test
    public void testGetAccess() {
        assertEquals(ACCESS, AccessExpressionUtil.getAccess());
    }

    // -- round-trip --

    @Test
    public void testRoundTripColumnVisibilityToAccessExpression() {
        ColumnVisibility original = new ColumnVisibility("A&B&C");
        AccessExpression ae = AccessExpressionUtil.toAccessExpression(original);
        ColumnVisibility roundTripped = AccessExpressionUtil.toColumnVisibility(ae);
        assertEquals(new String(original.getExpression()), new String(roundTripped.getExpression()));
    }

    // -- NormalizedExpression --

    @Test
    public void testNormalizedExpressionCompareTo() {
        var a = new AccessExpressionUtil.NormalizedExpression("A", ParsedAccessExpression.ExpressionType.AUTHORIZATION);
        var b = new AccessExpressionUtil.NormalizedExpression("B", ParsedAccessExpression.ExpressionType.AUTHORIZATION);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(a));
    }

    @Test
    public void testNormalizedExpressionEquals() {
        var a = new AccessExpressionUtil.NormalizedExpression("A", ParsedAccessExpression.ExpressionType.AUTHORIZATION);
        var aDuplicate = new AccessExpressionUtil.NormalizedExpression("A", ParsedAccessExpression.ExpressionType.AUTHORIZATION);
        assertEquals(a, aDuplicate);
    }
}
