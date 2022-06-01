package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class JexlFormattedStringBuildingVisitorTest {
	@Test
    public void testSimpleOr() throws ParseException {
        String query = "(BAR == 'foo' || BAR == 'blah')";
        String expected = "(\n\tBAR == 'foo' || \n\tBAR == 'blah'\n)\n";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        /**
         * (
				BAR == 'foo' ||
				BAR == 'blah'
			)
         */
        
        Assert.assertEquals(expected, builtQuery);
    }
	
	@Test
    public void testSimpleOr1() throws ParseException {
        String query = "BAR == 'foo' || BAR == 'blah'";
        String expected = "BAR == 'foo' || \nBAR == 'blah'\n";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        /**
         * BAR == 'foo' ||
         * BAR == 'blah'
         */
        Assert.assertEquals(expected, builtQuery);
    }
	
	@Test
    public void testSimpleOr2() throws ParseException {
        String query = "(BAR == 'foo' || BAR == 'blah' || BAR == 'test' || BAR == 'FOO')";
        String expected = "(\n\tBAR == 'foo' || \n\tBAR == 'blah' || \n\tBAR == 'test' || \n\tBAR == 'FOO'\n)\n";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        /**
         * (
				BAR == 'foo' ||
				BAR == 'blah' ||
				BAR == 'test' ||
				BAR == 'FOO'
			)
         */
        
        Assert.assertEquals(expected, builtQuery);
    }
	
	@Test
    public void testNestedOr() throws ParseException {
        String query = "((BAR == 'foo' || BAR == 'blah') || (BAR == 'test' || BAR == 'FOO'))";
        String expected = "(\n\t(\n\t\tBAR == 'foo' || \n\t\tBAR == 'blah'\n\t) || \n\t(\n\t\tBAR == 'test' || \n\t\tBAR == 'FOO'\n\t)\n)\n";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        /**
         * (
				(
					BAR == 'foo' ||
					BAR == 'blah'
				) ||
				(
					BAR == 'test' ||
					BAR == 'FOO'
				)
			)
         */
        
        Assert.assertEquals(expected, builtQuery);
    }
	
	@Test
	public void testNestedOrAnd() throws ParseException {
		String query = "((BAR == 'foo' && (BAR == 'blah' || BAR == 'test')) || ((BAR == 'abc' || BAR == '123') && BAR == 'FOO'))";
		String expected = "(\n\t(\n\t\tBAR == 'foo' && \n\t\t(\n\t\t\tBAR == 'blah' || \n\t\t\tBAR == 'test'\n\t\t)\n\t) "
				+ "|| \n\t(\n\t\t(\n\t\t\tBAR == 'abc' || \n\t\t\tBAR == '123'\n\t\t) && \n\t\tBAR == 'FOO'\n\t)\n)\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
		String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
		/**
		 * (
		 * 		(
		 * 			BAR == 'foo' &&
		 * 			(
		 * 				BAR == 'blah' ||
		 * 				BAR == 'test	
		 * 			)
		 * 		) ||
		 * 		(
		 * 			(
		 * 				BAR == 'abc' ||
		 * 				BAR == '123'
		 * 			) &&
		 * 			BAR == 'FOO'
		 * 		)
		 * )
		 */
		
		Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testComplicatedQuery() throws ParseException {
		String query = "((FIELD == 'some value') && "
				+ "((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && "
				+ "((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))) || "
				+ "(geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && "
				+ "(((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff')) || "
				+ "((_Bounded_ = true) && (POINT >= '1f20004a2000000000' && POINT <= '1f20004a2fffffffff')) || "
				+ "((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT <= '1f20004b0fffffffff')) || "
				+ "((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT <= '1f20004b3fffffffff')) || "
				+ "((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT <= '1f20004b47ffffffff')) || "
				+ "POINT == '1f20005b4000000000'))) && ((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah'))) && "
				+ "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10))))";
		String expected = "(\n" + 
        		"	(FIELD == 'some value') && \n" + 
        		"	(\n" + 
        		"		(\n" + 
        		"			geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && \n" + 
        		"			((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))\n" + 
        		"		) || \n" + 
        		"		(\n" + 
        		"			geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && \n" + 
        		"			(\n" + 
        		"				((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff')) || \n" + 
        		"				((_Bounded_ = true) && (POINT >= '1f20004a2000000000' && POINT <= '1f20004a2fffffffff')) || \n" + 
        		"				((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT <= '1f20004b0fffffffff')) || \n" + 
        		"				((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT <= '1f20004b3fffffffff')) || \n" + 
        		"				((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT <= '1f20004b47ffffffff')) || \n" + 
        		"				POINT == '1f20005b4000000000'\n" + 
        		"			)\n" + 
        		"		)\n" + 
        		"	) && \n" + 
        		"	(\n" + 
        		"		(_Eval_ = true) && \n" + 
        		"		(\n" + 
        		"			FOO == 'bar' && \n" + 
        		"			(\n" + 
        		"				BAR == 'foo' || \n" + 
        		"				BAR == 'blah'\n" + 
        		"			)\n" + 
        		"		)\n" + 
        		"	) && \n" + 
        		"	((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))\n" + 
        		")\n";
		/*
		 * (
				(FIELD == 'some value') && 
				(
					(
						geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && 
						((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))
					) || 
					(
						geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && 
						(
							((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff')) || 
							((_Bounded_ = true) && (POINT >= '1f20004a2000000000' && POINT <= '1f20004a2fffffffff')) || 
							((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT <= '1f20004b0fffffffff')) || 
							((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT <= '1f20004b3fffffffff')) || 
							((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT <= '1f20004b47ffffffff')) || 
							POINT == '1f20005b4000000000'
						)
					)
				) && 
				(
					(_Eval_ = true) && 
					(
						FOO == 'bar' && 
						(
							BAR == 'foo' || 
							BAR == 'blah'
						)
					)
				) && 
				((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))
			)
		 */
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testComplicatedQuerySimpler() throws ParseException {
		String query = "((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah'))) && ((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
		String expected = "(\n" + 
				"	(_Eval_ = true) && \n" + 
				"	(\n" + 
				"		FOO == 'bar' && \n" + 
				"		(\n" + 
				"			BAR == 'foo' || \n" + 
				"			BAR == 'blah'\n" + 
				"		)\n" + 
				"	)\n" + 
				") && \n" + 
				"((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))\n";
		/*
		 * (
				(_Eval_ = true) && 
				(
					FOO == 'bar' && 
					(
						BAR == 'foo' || 
						BAR == 'blah'
					)
				)
			) && 
			((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))
		 */
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testBoundedNodeRange() throws ParseException {
		// Marker bounded node anded with range, should be printed on same line
		String query = "((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff'))";
		String expected = "((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff'))\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testBoundedNodeSingleTerm() throws ParseException {
		// Marker bounded node anded with single term, should be printed on same line
		String query = "((_Bounded_ = true) && (POINT == '1f20004a1400000000'))";
		String expected = "((_Bounded_ = true) && (POINT == '1f20004a1400000000'))\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testMarkerNodesSingleTerm() throws ParseException {
		// Marker node anded with a single term, should be printed on same line
		String query = "(_Value_ = true) && (BAR == 'foo')";
		String expected = "(_Value_ = true) && (BAR == 'foo')\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testMarkerNodesSingleTerm1() throws ParseException {
		// Nested marker node anded with a single term, should be printed on same line
		String query = "((_Value_ = true) && (BAR == 'foo'))";
		String expected = "((_Value_ = true) && (BAR == 'foo'))\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testMarkerNodesSingleTerm2() throws ParseException {
		// Nested marker node anded with a single term, should be printed on same line
		String query = "(((_Value_ = true) && (BAR == 'foo')))";
		String expected = "(((_Value_ = true) && (BAR == 'foo')))\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	
	@Test
	public void testMarkerNodeAndedMultipleTerms() throws ParseException {
		// Marker node anded with subtree (not single term or range), should be printed on separate lines
		String query = "((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah')))";
		String expected = "(\n" + 
				"	(_Eval_ = true) && \n" + 
				"	(\n" + 
				"		FOO == 'bar' && \n" + 
				"		(\n" + 
				"			BAR == 'foo' || \n" + 
				"			BAR == 'blah'\n" + 
				"		)\n" + 
				"	)\n" + 
				")\n";
		/*
		 * (
				(_Eval_ = true) && 
				(
					FOO == 'bar' && 
					(
						BAR == 'foo' || 
						BAR == 'blah'
					)
				)
			)
		 */
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testMarkerNodeAndedMultipleTerms1() throws ParseException {
		// Marker node anded with subtree (not single term or range), should be printed on separate lines
		String query = "((_Eval_ = true) && (BAR == 'foo' || BAR == 'blah'))";
		String expected = "(\n" + 
				"	(_Eval_ = true) && \n" + 
				"	(\n" + 
				"		BAR == 'foo' || \n" + 
				"		BAR == 'blah'\n" + 
				"	)\n" + 
				")\n";
		/*
		 * (
				(_Eval_ = true) && 
				(
					BAR == 'foo' || 
					BAR == 'blah'
				)
			)
		 */
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
	
	@Test
	public void testNestedMarkerNodes() throws ParseException {
		// Nested marker nodes anded with a single term or range, should be printed as a single line
		String query = "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
		String expected = "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))\n";
		JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);
        
        Assert.assertEquals(expected, builtQuery);
	}
}
