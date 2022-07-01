package datawave.query.jexl.visitors;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.text.StringSubstitutor;
import org.junit.Assert;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class JexlFormattedStringBuildingVisitorTest {
    /***** Testing building the query with no decoration *****/
    @Test
    public void testSimpleOr() throws ParseException {
        String query = "(BAR == 'foo' || BAR == 'blah')";
        String expected = "(\n    BAR == 'foo' || \n    BAR == 'blah'\n)";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testSimpleOr1() throws ParseException {
        String query = "BAR == 'foo' || BAR == 'blah'";
        String expected = "BAR == 'foo' || \nBAR == 'blah'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testSimpleOr2() throws ParseException {
        String query = "(BAR == 'foo' || BAR == 'blah' || BAR == 'test' || BAR == 'FOO')";
        String expected = "(\n    BAR == 'foo' || \n    BAR == 'blah' || \n    BAR == 'test' || \n    BAR == 'FOO'\n)";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testNestedOr() throws ParseException {
        String query = "((BAR == 'foo' || BAR == 'blah') || (BAR == 'test' || BAR == 'FOO'))";
        String expected = "(\n    (\n        BAR == 'foo' || \n        BAR == 'blah'\n    ) || \n    (\n        BAR == 'test' || \n        BAR == 'FOO'\n    )\n)";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testNestedOrAnd() throws ParseException {
        String query = "((BAR == 'foo' && (BAR == 'blah' || BAR == 'test')) || ((BAR == 'abc' || BAR == '123') && BAR == 'FOO'))";
        String expected = "(\n    (\n        BAR == 'foo' && \n        (\n            BAR == 'blah' || \n            BAR == 'test'\n        )\n    ) "
                        + "|| \n    (\n        (\n            BAR == 'abc' || \n            BAR == '123'\n        ) && \n        BAR == 'FOO'\n    )\n)";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testComplicatedQuery() throws ParseException {
        String query = "((FIELD == 'some value') && " + "((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && "
                        + "((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))) || "
                        + "(geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && "
                        + "(((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff')) || "
                        + "((_Bounded_ = true) && (POINT >= '1f20004a2000000000' && POINT <= '1f20004a2fffffffff')) || "
                        + "((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT <= '1f20004b0fffffffff')) || "
                        + "((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT <= '1f20004b3fffffffff')) || "
                        + "((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT <= '1f20004b47ffffffff')) || "
                        + "POINT == '1f20005b4000000000'))) && ((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah'))) && "
                        + "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10))))";
        String expected = "(\n" + "    (FIELD == 'some value') && \n" + "    (\n" + "        (\n"
                        + "            geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && \n"
                        + "            ((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))\n" + "        ) || \n" + "        (\n"
                        + "            geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && \n" + "            (\n"
                        + "                ((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff')) || \n"
                        + "                ((_Bounded_ = true) && (POINT >= '1f20004a2000000000' && POINT <= '1f20004a2fffffffff')) || \n"
                        + "                ((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT <= '1f20004b0fffffffff')) || \n"
                        + "                ((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT <= '1f20004b3fffffffff')) || \n"
                        + "                ((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT <= '1f20004b47ffffffff')) || \n"
                        + "                POINT == '1f20005b4000000000'\n" + "            )\n" + "        )\n" + "    ) && \n" + "    (\n"
                        + "        (_Eval_ = true) && \n" + "        (\n" + "            FOO == 'bar' && \n" + "            (\n"
                        + "                BAR == 'foo' || \n" + "                BAR == 'blah'\n" + "            )\n" + "        )\n" + "    ) && \n"
                        + "    ((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))\n" + ")";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testComplicatedQuerySimpler() throws ParseException {
        String query = "((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah'))) && ((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
        String expected = "(\n" + "    (_Eval_ = true) && \n" + "    (\n" + "        FOO == 'bar' && \n" + "        (\n" + "            BAR == 'foo' || \n"
                        + "            BAR == 'blah'\n" + "        )\n" + "    )\n" + ") && \n"
                        + "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testBoundedNodeRange() throws ParseException {
        // Marker bounded node anded with range, should be printed on same line
        String query = "((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff'))";
        String expected = "((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT <= '1f20004a1fffffffff'))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testBoundedNodeSingleTerm() throws ParseException {
        // Marker bounded node anded with single term, should be printed on same line
        String query = "((_Bounded_ = true) && (POINT == '1f20004a1400000000'))";
        String expected = "((_Bounded_ = true) && (POINT == '1f20004a1400000000'))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMarkerNodesSingleTerm() throws ParseException {
        // Marker node anded with a single term, should be printed on same line
        String query = "(_Value_ = true) && (BAR == 'foo')";
        String expected = "(_Value_ = true) && (BAR == 'foo')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMarkerNodesSingleTerm1() throws ParseException {
        // Nested marker node anded with a single term, should be printed on same line
        String query = "((_Value_ = true) && (BAR == 'foo'))";
        String expected = "((_Value_ = true) && (BAR == 'foo'))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMarkerNodesSingleTerm2() throws ParseException {
        // Nested marker node anded with a single term, should be printed on same line
        String query = "(((_Value_ = true) && (BAR == 'foo')))";
        String expected = "(((_Value_ = true) && (BAR == 'foo')))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMarkerNodeAndedMultipleTerms() throws ParseException {
        // Marker node anded with subtree (not single term or range), should be printed on separate lines
        String query = "((_Eval_ = true) && (FOO == 'bar' && (BAR == 'foo' || BAR == 'blah')))";
        String expected = "(\n" + "    (_Eval_ = true) && \n" + "    (\n" + "        FOO == 'bar' && \n" + "        (\n" + "            BAR == 'foo' || \n"
                        + "            BAR == 'blah'\n" + "        )\n" + "    )\n" + ")";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMarkerNodeAndedMultipleTerms1() throws ParseException {
        // Marker node anded with subtree (not single term or range), should be printed on separate lines
        String query = "((_Eval_ = true) && (BAR == 'foo' || BAR == 'blah'))";
        String expected = "(\n" + "    (_Eval_ = true) && \n" + "    (\n" + "        BAR == 'foo' || \n" + "        BAR == 'blah'\n" + "    )\n" + ")";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testNestedMarkerNodes() throws ParseException {
        // Nested marker nodes anded with a single term or range, should be printed as a single line
        String query = "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
        String expected = "((_Value_ = true) && ((_Bounded_ = true) && (NUM >= 0 && NUM <= 10)))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMultiParensIndentation() throws ParseException {
        String query = "((((FOO == 'BAR' && ((_Eval_ = true) && (THIS == 'THAT'))))))";
        String expected = "((((\n    FOO == 'BAR' && \n    ((_Eval_ = true) && (THIS == 'THAT'))\n))))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMultiParensIndentation1() throws ParseException {
        String query = "((((FOO == 'BAR' && ((_Eval_ = true) && (THIS == 'THAT')))))) || ((((FOO == 'BAR' && ((_Eval_ = true) && (THIS == 'THAT'))))))";
        String expected = "((((\n    FOO == 'BAR' && \n    ((_Eval_ = true) && (THIS == 'THAT'))\n)))) || "
                        + "\n((((\n    FOO == 'BAR' && \n    ((_Eval_ = true) && (THIS == 'THAT'))\n))))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }

    @Test
    public void testMultiParensIndentation2() throws ParseException {
        String query = "((((FOO == 'BAR' && ((_Eval_ = true) && (THIS == 'THAT')))))) && ((((FOO == 'BAR' && ((_Eval_ = true) && (THIS == 'THAT'))))))";
        String expected = "((((\n    FOO == 'BAR' && \n    ((_Eval_ = true) && (THIS == 'THAT'))\n)))) && "
                        + "\n((((\n    FOO == 'BAR' && \n    ((_Eval_ = true) && (THIS == 'THAT'))\n))))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String builtQuery = JexlFormattedStringBuildingVisitor.buildQuery(node);

        Assert.assertEquals(expected, builtQuery);
    }
    
    /***** Testing building the query with Bash decoration *****/
    
    /**
     * Tests the building and bash decoration of a complicated query which uses all colors as specified in BashDecorator
     * 
     * @throws ParseException
     */
    @Test
    public void testComplicatedQueryBashDecoration() throws ParseException {
        String query = "(((FIELD == 'some value') && ((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && ((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))) || (geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && (((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT != '1f20004a1fffffffff')) || ((_Bounded_ = true) && (POINT >= 20 && POINT <= 30)) || ((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT >= '1f20004b0fffffffff')) || ((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT < '1f20004b3fffffffff')) || ((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT > '1f20004b47ffffffff')) || POINT == 99))) && ((_Eval_ = false) && (FOO =~ 'bar' && (BAR !~ 'foo' || BAR == 'blah'))) && (((_Value_ = true).size() == 4) && ((_Bounded_ = true = true) && (NUM == null && !(NUM <= 10))))) && ((POINT == 8 + 2) || (POINT == -15) || (POINT == 8 * 2) || (POINT == 8 / 2) || (POINT == 8 % 2) || (POINT.max() == 5)))";
        Map<String,String> map = new HashMap<>();
        map.put("noColor", BashDecorator.NC);
        map.put("fieldColor", BashDecorator.FIELD_COLOR);
        map.put("equalColor", BashDecorator.EQUAL_COLOR);
        map.put("stringColor", BashDecorator.STRING_COLOR);
        map.put("andColor", BashDecorator.AND_COLOR);
        map.put("namespaceColor", BashDecorator.NAMESPACE_COLOR);
        map.put("functionColor", BashDecorator.FUNCTION_COLOR);
        map.put("markerColor", BashDecorator.MARKER_COLOR);
        map.put("assignColor", BashDecorator.ASSIGN_COLOR);
        map.put("booleanColor", BashDecorator.BOOLEAN_COLOR);
        map.put("greaterEqualColor", BashDecorator.GREATER_EQUAL_COLOR);
        map.put("orColor", BashDecorator.OR_COLOR);
        map.put("notEqualColor", BashDecorator.NOT_EQUAL_COLOR);
        map.put("lessEqualColor", BashDecorator.LESS_EQUAL_COLOR);
        map.put("numberColor", BashDecorator.NUMBER_COLOR);
        map.put("lessThanColor", BashDecorator.LESS_THAN_COLOR);
        map.put("greaterThanColor", BashDecorator.GREATER_THAN_COLOR);
        map.put("ERColor", BashDecorator.ER_COLOR);
        map.put("NRColor", BashDecorator.NR_COLOR);
        map.put("methodColor", BashDecorator.METHOD_COLOR);
        map.put("nullColor", BashDecorator.NULL_COLOR);
        map.put("notColor", BashDecorator.NOT_COLOR);
        map.put("arithmeticOpColor", BashDecorator.ARITHMETIC_OP_COLOR);
        StringSubstitutor strSub = new StringSubstitutor(map);
        String template = "(\n"
                        + "    (\n"
                        + "        (${fieldColor}FIELD${noColor}${equalColor} == ${noColor}${stringColor}'some value'${noColor})${andColor} && ${noColor}\n"
                        + "        (\n"
                        + "            (\n"
                        + "                ${namespaceColor}geowave${noColor}:${functionColor}intersects${noColor}(${fieldColor}GEO${noColor}, ${stringColor}'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))'${noColor})${andColor} && ${noColor}\n"
                        + "                ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}GEO${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'2.0|0.5'${noColor}${andColor} && ${noColor}${fieldColor}GEO${noColor}${lessEqualColor} <= ${noColor}${stringColor}'10.0|1.5'${noColor}))\n"
                        + "            )${orColor} || ${noColor}\n"
                        + "            (\n"
                        + "                ${namespaceColor}geowave${noColor}:${functionColor}intersects${noColor}(${fieldColor}POINT${noColor}, ${stringColor}'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))'${noColor})${andColor} && ${noColor}\n"
                        + "                (\n"
                        + "                    ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'1f20004a1400000000'${noColor}${andColor} && ${noColor}${fieldColor}POINT${noColor}${notEqualColor} != ${noColor}${stringColor}'1f20004a1fffffffff'${noColor}))${orColor} || ${noColor}\n"
                        + "                    ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${numberColor}20${noColor}${andColor} && ${noColor}${fieldColor}POINT${noColor}${lessEqualColor} <= ${noColor}${numberColor}30${noColor}))${orColor} || ${noColor}\n"
                        + "                    ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'1f20004a6000000000'${noColor}${andColor} && ${noColor}${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'1f20004b0fffffffff'${noColor}))${orColor} || ${noColor}\n"
                        + "                    ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'1f20004b3000000000'${noColor}${andColor} && ${noColor}${fieldColor}POINT${noColor}${lessThanColor} < ${noColor}${stringColor}'1f20004b3fffffffff'${noColor}))${orColor} || ${noColor}\n"
                        + "                    ((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${andColor} && ${noColor}(${fieldColor}POINT${noColor}${greaterEqualColor} >= ${noColor}${stringColor}'1f20004b4000000000'${noColor}${andColor} && ${noColor}${fieldColor}POINT${noColor}${greaterThanColor} > ${noColor}${stringColor}'1f20004b47ffffffff'${noColor}))${orColor} || ${noColor}\n"
                        + "                    ${fieldColor}POINT${noColor}${equalColor} == ${noColor}${numberColor}99${noColor}\n"
                        + "                )\n"
                        + "            )\n"
                        + "        )${andColor} && ${noColor}\n"
                        + "        (\n"
                        + "            (${markerColor}_Eval_${noColor}${assignColor} = ${noColor}${booleanColor}false${noColor})${andColor} && ${noColor}\n"
                        + "            (\n"
                        + "                ${fieldColor}FOO${noColor}${ERColor} =~ ${noColor}${stringColor}'bar'${noColor}${andColor} && ${noColor}\n"
                        + "                (\n"
                        + "                    ${fieldColor}BAR${noColor}${NRColor} !~ ${noColor}${stringColor}'foo'${noColor}${orColor} || ${noColor}\n"
                        + "                    ${fieldColor}BAR${noColor}${equalColor} == ${noColor}${stringColor}'blah'${noColor}\n"
                        + "                )\n"
                        + "            )\n"
                        + "        )${andColor} && ${noColor}\n"
                        + "        (((${markerColor}_Value_${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor})${methodColor}.size() ${noColor}${equalColor} == ${noColor}${numberColor}4${noColor})${andColor} && ${noColor}((${markerColor}_Bounded_${noColor}${assignColor} = ${noColor}(${booleanColor}true${noColor}${assignColor} = ${noColor}${booleanColor}true${noColor}))${andColor} && ${noColor}(${fieldColor}NUM${noColor}${equalColor} == ${noColor}${nullColor}null${noColor}${andColor} && ${noColor}${notColor}!${noColor}(${fieldColor}NUM${noColor}${lessEqualColor} <= ${noColor}${numberColor}10${noColor}))))\n"
                        + "    )${andColor} && ${noColor}\n"
                        + "    (\n"
                        + "        (${fieldColor}POINT${noColor}${equalColor} == ${noColor}${numberColor}8${noColor}${arithmeticOpColor}+${noColor}${numberColor}2${noColor})${orColor} || ${noColor}\n"
                        + "        (${fieldColor}POINT${noColor}${equalColor} == ${noColor}${arithmeticOpColor}-${noColor}${numberColor}15${noColor})${orColor} || ${noColor}\n"
                        + "        (${fieldColor}POINT${noColor}${equalColor} == ${noColor}${numberColor}8${noColor}${arithmeticOpColor} * ${noColor}${numberColor}2${noColor})${orColor} || ${noColor}\n"
                        + "        (${fieldColor}POINT${noColor}${equalColor} == ${noColor}${numberColor}8${noColor}${arithmeticOpColor} / ${noColor}${numberColor}2${noColor})${orColor} || ${noColor}\n"
                        + "        (${fieldColor}POINT${noColor}${equalColor} == ${noColor}${numberColor}8${noColor}${arithmeticOpColor} % ${noColor}${numberColor}2${noColor})${orColor} || ${noColor}\n"
                        + "        (${fieldColor}POINT${noColor}${methodColor}.max()${noColor}${equalColor} == ${noColor}${numberColor}5${noColor})\n"
                        + "    )\n" + ")";
        String expected = strSub.replace(template);
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String decoratedBuiltQuery = JexlFormattedStringBuildingVisitor.buildDecoratedQuery(node, false, new BashDecorator());
        
        Assert.assertEquals(expected, decoratedBuiltQuery);
    }
    
    /***** Testing building the query with HTML decoration *****/
    
    /**
     * Tests the building and HTML decoration of a complicated query which uses all the CSS classes as specified in HtmlDecorator
     * 
     * @throws ParseException
     */
    @Test
    public void testComplicatedQueryHtmlDecoration() throws ParseException {
        String query = "(((FIELD == 'some value') && ((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && ((_Bounded_ = true) && (GEO >= '2.0|0.5' && GEO <= '10.0|1.5'))) || (geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && (((_Bounded_ = true) && (POINT >= '1f20004a1400000000' && POINT != '1f20004a1fffffffff')) || ((_Bounded_ = true) && (POINT >= 20 && POINT <= 30)) || ((_Bounded_ = true) && (POINT >= '1f20004a6000000000' && POINT >= '1f20004b0fffffffff')) || ((_Bounded_ = true) && (POINT >= '1f20004b3000000000' && POINT < '1f20004b3fffffffff')) || ((_Bounded_ = true) && (POINT >= '1f20004b4000000000' && POINT > '1f20004b47ffffffff')) || POINT == 99))) && ((_Eval_ = false) && (FOO =~ 'bar' && (BAR !~ 'foo' || BAR == 'blah'))) && (((_Value_ = true).size() == 4) && ((_Bounded_ = true = true) && (NUM == null && !(NUM <= 10))))) && ((POINT == 8 + 2) || (POINT == -15) || (POINT == 8 * 2) || (POINT == 8 / 2) || (POINT == 8 % 2) || (POINT.max() == 5)))";
        String expected = "(\n"
                        + "    (\n"
                        + "        (<span class=\"field\">FIELD</span><span class=\"equal-op\"> == </span><span class=\"string-value\">'some value'</span>)<span class=\"and-op\"> && </span>\n"
                        + "        (\n"
                        + "            (\n"
                        + "                <span class=\"function-namespace\">geowave</span>:<span class=\"function\">intersects</span>(<span class=\"field\">GEO</span>, <span class=\"string-value\">'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))'</span>)<span class=\"and-op\"> && </span>\n"
                        + "                ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">GEO</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'2.0|0.5'</span><span class=\"and-op\"> && </span><span class=\"field\">GEO</span><span class=\"less-than-equal-op\"> <= </span><span class=\"string-value\">'10.0|1.5'</span>))\n"
                        + "            )<span class=\"or-op\"> || </span>\n"
                        + "            (\n"
                        + "                <span class=\"function-namespace\">geowave</span>:<span class=\"function\">intersects</span>(<span class=\"field\">POINT</span>, <span class=\"string-value\">'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))'</span>)<span class=\"and-op\"> && </span>\n"
                        + "                (\n"
                        + "                    ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'1f20004a1400000000'</span><span class=\"and-op\"> && </span><span class=\"field\">POINT</span><span class=\"not-equal-op\"> != </span><span class=\"string-value\">'1f20004a1fffffffff'</span>))<span class=\"or-op\"> || </span>\n"
                        + "                    ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"numeric-value\">20</span><span class=\"and-op\"> && </span><span class=\"field\">POINT</span><span class=\"less-than-equal-op\"> <= </span><span class=\"numeric-value\">30</span>))<span class=\"or-op\"> || </span>\n"
                        + "                    ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'1f20004a6000000000'</span><span class=\"and-op\"> && </span><span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'1f20004b0fffffffff'</span>))<span class=\"or-op\"> || </span>\n"
                        + "                    ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'1f20004b3000000000'</span><span class=\"and-op\"> && </span><span class=\"field\">POINT</span><span class=\"less-than-op\"> < </span><span class=\"string-value\">'1f20004b3fffffffff'</span>))<span class=\"or-op\"> || </span>\n"
                        + "                    ((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"and-op\"> && </span>(<span class=\"field\">POINT</span><span class=\"greater-than-equal-op\"> >= </span><span class=\"string-value\">'1f20004b4000000000'</span><span class=\"and-op\"> && </span><span class=\"field\">POINT</span><span class=\"greater-than-op\"> > </span><span class=\"string-value\">'1f20004b47ffffffff'</span>))<span class=\"or-op\"> || </span>\n"
                        + "                    <span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">99</span>\n"
                        + "                )\n"
                        + "            )\n"
                        + "        )<span class=\"and-op\"> && </span>\n"
                        + "        (\n"
                        + "            (<span class=\"query-property-marker\">_Eval_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">false</span>)<span class=\"and-op\"> && </span>\n"
                        + "            (\n"
                        + "                <span class=\"field\">FOO</span><span class=\"ER-op\"> =~ </span><span class=\"string-value\">'bar'</span><span class=\"and-op\"> && </span>\n"
                        + "                (\n"
                        + "                    <span class=\"field\">BAR</span><span class=\"NR-op\"> !~ </span><span class=\"string-value\">'foo'</span><span class=\"or-op\"> || </span>\n"
                        + "                    <span class=\"field\">BAR</span><span class=\"equal-op\"> == </span><span class=\"string-value\">'blah'</span>\n"
                        + "                )\n"
                        + "            )\n"
                        + "        )<span class=\"and-op\"> && </span>\n"
                        + "        (((<span class=\"query-property-marker\">_Value_</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>)<span class=\"method\">.size() </span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">4</span>)<span class=\"and-op\"> && </span>((<span class=\"query-property-marker\">_Bounded_</span><span class=\"assign-op\"> = </span>(<span class=\"boolean-value\">true</span><span class=\"assign-op\"> = </span><span class=\"boolean-value\">true</span>))<span class=\"and-op\"> && </span>(<span class=\"field\">NUM</span><span class=\"equal-op\"> == </span><span class=\"null-value\">null</span><span class=\"and-op\"> && </span><span class=\"not-op\">!</span>(<span class=\"field\">NUM</span><span class=\"less-than-equal-op\"> <= </span><span class=\"numeric-value\">10</span>))))\n"
                        + "    )<span class=\"and-op\"> && </span>\n"
                        + "    (\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">8</span><span class=\"add-op\">+</span><span class=\"numeric-value\">2</span>)<span class=\"or-op\"> || </span>\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"unary-minus\">-</span><span class=\"numeric-value\">15</span>)<span class=\"or-op\"> || </span>\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">8</span><span class=\"mul-op\"> * </span><span class=\"numeric-value\">2</span>)<span class=\"or-op\"> || </span>\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">8</span><span class=\"div-op\"> / </span><span class=\"numeric-value\">2</span>)<span class=\"or-op\"> || </span>\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">8</span><span class=\"mod-op\"> % </span><span class=\"numeric-value\">2</span>)<span class=\"or-op\"> || </span>\n"
                        + "        (<span class=\"field\">POINT</span><span class=\"method\">.max()</span><span class=\"equal-op\"> == </span><span class=\"numeric-value\">5</span>)\n"
                        + "    )\n" + ")";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        String decoratedBuiltQuery = JexlFormattedStringBuildingVisitor.buildDecoratedQuery(node, false, new HtmlDecorator());
        
        Assert.assertEquals(expected, decoratedBuiltQuery);
    }
}
