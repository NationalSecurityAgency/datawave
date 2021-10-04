package datawave.query.jexl.functions;

import com.google.common.collect.Lists;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

public class EvaluationPhaseFilterFunctionsTest {
    
    private ValueTuple makeValueTuple(String csv) {
        String[] tokens = csv.split(",");
        String field = tokens[0];
        Type<String> type = new LcNoDiacriticsType(tokens[1]);
        TypeAttribute<String> typeAttribute = new TypeAttribute<>(type, new Key(), true);
        String normalized = tokens[2];
        return new ValueTuple(field, type, normalized, typeAttribute);
    }
    
    @Test
    public void testIncludeRegex() {
        // @formatter:off
        // will return the first (thus only one) match
        Assert.assertTrue(EvaluationPhaseFilterFunctions.includeRegex(
                Lists.newArrayList(
                        makeValueTuple("FOO.1,BAR,bar"),
                        makeValueTuple("FOO.2,BAR,bar"),
                        makeValueTuple("FOO.3,BAZ,baz")),
                "ba.*").size() == 1);
        // will return the set of all unique matches
        Assert.assertTrue(EvaluationPhaseFilterFunctions.getAllMatches(
                Lists.newArrayList(
                        makeValueTuple("FOO.1,BAR,bar"),
                        makeValueTuple("FOO.2,BAZ,baz"),
                        makeValueTuple("FOO.3,BAR,bar")),
                "ba.*").size() == 3);
        // @formatter:on
    }
    
    @Test
    public void testMatchesAtLeastCountOf() {
        // @formatter:off
        Assert.assertTrue(EvaluationPhaseFilterFunctions.matchesAtLeastCountOf(
                3,
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE"
        ).size() == 3);

        Assert.assertTrue(EvaluationPhaseFilterFunctions.matchesAtLeastCountOf(
                3,
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,GROUCHO,groucho")),
                "MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE"
        ).size() == 0);
        // @formatter:on
    }
    
    @Test
    public void testRightOf() {
        // @formatter:off
        String[] inputs = {
                "NAME.grandparent_0.parent_0.child_0",
                "NAME.grandparent_0.parent_0.child_0",
                "NAME.gggparent.ggparent.grandparent_0.parent_0.child_0"
        };
        String[] expected = {
                "child_0",
                "parent_0.child_0",
                "ggparent.grandparent_0.parent_0.child_0"
        };
        int[] groupNumber = new int[] {
                0,
                1,
                3
        };
        // @formatter:on
        for (int i = 0; i < inputs.length; i++) {
            String match = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(inputs[i], groupNumber[i]);
            Assert.assertEquals(match, expected[i]);
        }
    }
    
    @Test
    public void testLeftOf() {
        // @formatter:off
        String[] inputs = {
                "NAME.gggparent.ggparent.grandparent_0.parent_0.child_0",
                "NAME.gggparent.ggparent.grandparent_0.parent_0.child_0",
                "NAME.gggparent.ggparent.grandparent_0.parent_0.child_0"
        };
        String[] expected = {
                "gggparent.ggparent.grandparent_0.parent_0",
                "gggparent.ggparent.grandparent_0",
                "gggparent"
        };
        int[] groupNumber = new int[] {
                0,
                1,
                3
        };
        // @formatter:on
        for (int i = 0; i < inputs.length; i++) {
            String match = EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(inputs[i], groupNumber[i]);
            Assert.assertEquals(expected[i], match);
        }
    }
    
    @Test
    public void testCompareLT() {
        // @formatter:off
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "<", "ANY", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), "<", "ANY",null));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<", "ANY",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "<", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<", "ALL",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "<", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "<", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                "<", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "<", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
    
    @Test
    public void testCompareLE() {
        // @formatter:off
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "<=", "ANY", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), "<=", "ANY",null));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<=", "ANY",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "<=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<=", "ALL",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "<=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "<=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "<=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                "<=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "<=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
    
    @Test
    public void testCompareGT() {
        // @formatter:off
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, ">", "ANY", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), ">", "ANY",null));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">", "ANY",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                ">", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">", "ALL",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                ">", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                ">", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                ">", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                ">", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
    
    @Test
    public void testCompareGE() {
        // @formatter:off
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, ">=", "ANY", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), ">=", "ANY",null));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">=", "ANY",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                ">=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">=", "ALL",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                ">=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                ">=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                ">=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                ">=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                ">=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
    
    @Test
    public void testCompareEQ() {
        testCompareEQHelper("=");
    }
    
    @Test
    public void testCompareEQ2() {
        testCompareEQHelper("==");
    }
    
    private void testCompareEQHelper(String operator) {
        // @formatter:off
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(null, operator, "ANY", Lists.newArrayList()));
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), operator, "ANY",null));
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(null, operator, "ANY",null));
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(null, operator, "ALL", Lists.newArrayList()));
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), operator, "ALL",null));
        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(null, operator, "ALL", null));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                operator, "ANY",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                operator, "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                operator, "ALL",
                Lists.newArrayList()
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                operator, "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                operator, "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                operator, "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                operator, "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                operator, "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
    
    @Test
    public void testCompareNE() {
        // @formatter:off
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "!=", "ANY", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), "!=", "ANY",null));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "!=", "ANY",null));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "!=", "ALL", Lists.newArrayList()));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(Lists.newArrayList(), "!=", "ALL",null));
        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(null, "!=", "ALL", null));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "!=", "ANY",
                Lists.newArrayList()
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "!=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "!=", "ALL",
                Lists.newArrayList()
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(),
                "!=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")),
                "!=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "!=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe"))
        ));

        Assert.assertFalse(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe")),
                "!=", "ANY",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                )
        ));

        Assert.assertTrue(EvaluationPhaseFilterFunctions.compare(
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"),
                        makeValueTuple("STOOGE.2,LARRY,larry"),
                        makeValueTuple("STOOGE.3,JOE,joe")
                ),
                "!=", "ALL",
                Lists.newArrayList(
                        makeValueTuple("STOOGE.1,MOE,moe"))
        ));
        // @formatter:on
    }
}
