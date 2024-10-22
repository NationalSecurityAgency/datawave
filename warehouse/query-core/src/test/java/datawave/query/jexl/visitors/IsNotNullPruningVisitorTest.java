package datawave.query.jexl.visitors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;

public class IsNotNullPruningVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    // test pruning single 'is not null' term via single anchor field

    @Test
    public void testNotNullTermAndEq() {
        String query = "!(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndEr() {
        String query = "!(FOO == null) && FOO =~ 'ba.*'";
        String expected = "FOO =~ 'ba.*'";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndIncludeRegex() {
        String query = "!(FOO == null) && filter:includeRegex(FOO, 'ba.*')";
        String expected = "filter:includeRegex(FOO, 'ba.*')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndGetAllMatches() {
        String query = "!(FOO == null) && filter:getAllMatches(FOO, 'ba.*')";
        String expected = "filter:getAllMatches(FOO, 'ba.*')";
        test(query, expected);
    }

    // test pruning single 'is not null' term via multiple anchor fields

    @Test
    public void testNotNullTermAndManyEqs() {
        String query = "!(FOO == null) && FOO == 'bar' && FOO == 'baz'";
        String expected = "FOO == 'bar' && FOO == 'baz'";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndManyErs() {
        String query = "!(FOO == null) && FOO =~ 'br.*' && FOO =~ 'bz.*'";
        String expected = "FOO =~ 'br.*' && FOO =~ 'bz.*'";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndManyIncludeRegex() {
        String query = "!(FOO == null) && filter:includeRegex(FOO, 'br.*') && filter:includeRegex(FOO, 'bz.*')";
        String expected = "filter:includeRegex(FOO, 'br.*') && filter:includeRegex(FOO, 'bz.*')";
        test(query, expected);
    }

    // test pruning 'is not null' term, do not touch extra terms

    @Test
    public void testNotNullTermAndEqAndOthers() {
        String query = "!(FOO == null) && FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        String expected = "FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndErAndOthers() {
        String query = "!(FOO == null) && FOO =~ 'br.*' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        String expected = "FOO =~ 'br.*' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndIncludeRegexAndOthers() {
        String query = "!(FOO == null) && filter:includeRegex(FOO, 'ba.*') && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        String expected = "filter:includeRegex(FOO, 'ba.*') && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        test(query, expected);
    }

    // test pruning duplicate 'is not null' terms

    @Test
    public void testMultipleNotNullTermsAndEq() {
        String query = "!(FOO == null) && !(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);

        // unordered
        query = "!(FOO == null) && FOO == 'bar' && !(FOO == null)";
        expected = "FOO == 'bar'";
        test(query, expected);
    }

    @Test
    public void testMultipleNotNullTermsAndEr() {
        String query = "!(FOO == null) && !(FOO == null) && FOO =~ 'ba.*'";
        String expected = "FOO =~ 'ba.*'";
        test(query, expected);

        // unordered
        query = "!(FOO == null) && FOO =~ 'ba.*' && !(FOO == null)";
        expected = "FOO =~ 'ba.*'";
        test(query, expected);
    }

    @Test
    public void testMultipleNotNullTermsAndIncludeRegex() {
        String query = "!(FOO == null) && !(FOO == null) && filter:includeRegex(FOO, 'ba.*')";
        String expected = "filter:includeRegex(FOO, 'ba.*')";
        test(query, expected);

        // unordered
        query = "!(FOO == null) && filter:includeRegex(FOO, 'ba.*') && !(FOO == null)";
        expected = "filter:includeRegex(FOO, 'ba.*')";
        test(query, expected);
    }

    @Test
    public void testMultipleGetMatchesTermsAndIncludeRegex() {
        String query = "!(FOO == null) && !(FOO == null) && filter:getAllMatches(FOO, 'ba.*')";
        String expected = "filter:getAllMatches(FOO, 'ba.*')";
        test(query, expected);

        // unordered
        query = "!(FOO == null) && filter:getAllMatches(FOO, 'ba.*') && !(FOO == null)";
        expected = "filter:getAllMatches(FOO, 'ba.*')";
        test(query, expected);
    }

    // prune single 'is not null' term from multiple unique terms

    @Test
    public void testMultipleUniqueNotNullTermsAndEq() {
        String query = "!(FOO == null) && !(FOO2 == null) && FOO == 'bar'";
        String expected = "!(FOO2 == null) && FOO == 'bar'";
        test(query, expected);
    }

    @Test
    public void testMultipleUniqueNotNullTermsAndEr() {
        String query = "!(FOO == null) && !(FOO2 == null) && FOO =~ 'br.*'";
        String expected = "!(FOO2 == null) && FOO =~ 'br.*'";
        test(query, expected);
    }

    @Test
    public void testMultipleUniqueNotNullTermsAndIncludeRegex() {
        String query = "!(FOO == null) && !(FOO2 == null) && filter:includeRegex(FOO, 'br.*')";
        String expected = "!(FOO2 == null) && filter:includeRegex(FOO, 'br.*')";
        test(query, expected);
    }

    @Test
    public void testMultipleUniqueNotNullTermsAndAllMatches() {
        String query = "!(FOO == null) && !(FOO2 == null) && filter:getAllMatches(FOO, 'br.*')";
        String expected = "!(FOO2 == null) && filter:getAllMatches(FOO, 'br.*')";
        test(query, expected);
    }

    // prune multiple unique 'is not null' terms

    @Test
    public void testMultipleUniqueNotNullTermsAndMatchingEqs() {
        String query = "!(FOO == null) && !(FOO2 == null) && FOO == 'bar' && FOO2 == 'baz'";
        String expected = "FOO == 'bar' && FOO2 == 'baz'";
        test(query, expected);

        // order matters not
        query = "FOO2 == 'baz' && !(FOO == null) && FOO == 'bar' && !(FOO2 == null)";
        expected = "FOO2 == 'baz' && FOO == 'bar'";
        test(query, expected);
    }

    @Test
    public void testMultipleUniqueNotNullTermsAndMatchingErs() {
        String query = "!(FOO == null) && !(FOO2 == null) && FOO =~ 'br.*' && FOO2 =~ 'bz.*'";
        String expected = "FOO =~ 'br.*' && FOO2 =~ 'bz.*'";
        test(query, expected);

        // order matters not
        query = "FOO2 =~ 'bz.*' && !(FOO == null) && FOO =~ 'br.*' && !(FOO2 == null)";
        expected = "FOO2 =~ 'bz.*' && FOO =~ 'br.*'";
        test(query, expected);
    }

    @Test
    public void testUnionOfNotNullTermsAndIncludeRegex() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'xy.*')";
        String expected = "filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'xy.*')";
        test(query, expected);

        // order should not matter
        query = "filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'xy.*') && (!(FOO == null) || !(FOO2 == null))";
        expected = "filter:includeRegex(FOO, 'ba.*') && filter:includeRegex(FOO2, 'xy.*')";
        test(query, expected);
    }

    @Test
    public void testUnionOfNotNullTermsAndAllMatches() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && filter:getAllMatches(FOO, 'ba.*') && filter:getAllMatches(FOO2, 'xy.*')";
        String expected = "filter:getAllMatches(FOO, 'ba.*') && filter:getAllMatches(FOO2, 'xy.*')";
        test(query, expected);

        // order should not matter
        query = "filter:getAllMatches(FOO, 'ba.*') && filter:getAllMatches(FOO2, 'xy.*') && (!(FOO == null) || !(FOO2 == null))";
        expected = "filter:getAllMatches(FOO, 'ba.*') && filter:getAllMatches(FOO2, 'xy.*')";
        test(query, expected);
    }

    // test pruning nested expressions

    @Test
    public void testNestedNotNullTermAndEq() {
        String query = "(!(FOO == null) && FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);

        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && FOO == 'bar')";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (FOO == 'bar')";
        test(query, expected);
    }

    @Test
    public void testNestedNotNullTermAndEr() {
        String query = "(!(FOO == null) && FOO =~ 'ba.*') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(FOO =~ 'ba.*') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);

        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && FOO =~ 'ba.*')";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (FOO =~ 'ba.*')";
        test(query, expected);
    }

    @Test
    public void testNestedNotNullTermAndIncludeRegex() {
        String query = "(!(FOO == null) && filter:includeRegex(FOO, 'ba.*')) || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(filter:includeRegex(FOO, 'ba.*')) || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);

        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && filter:includeRegex(FOO, 'ba.*'))";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (filter:includeRegex(FOO, 'ba.*'))";
        test(query, expected);
    }

    @Test
    public void testNestedNotNullTermAndAllMatches() {
        String query = "(!(FOO == null) && filter:getAllMatches(FOO, 'ba.*')) || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(filter:getAllMatches(FOO, 'ba.*')) || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);

        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && filter:getAllMatches(FOO, 'ba.*'))";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (filter:getAllMatches(FOO, 'ba.*'))";
        test(query, expected);
    }

    // test pruning 'is not null' term via union of same field

    @Test
    public void testNotNullTermAndUnionOfEqs() {
        // every field in the union matches the 'not null' term's field, thus we can still prune
        String query = "!(FOO == null) && (FOO == 'bar' || FOO == 'baz')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);

        // with extras
        query = "!(FOO == null) && (FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndUnionOfErs() {
        // every field in the union matches the 'not null' term's field, thus we can still prune
        String query = "!(FOO == null) && (FOO =~ 'br.*' || FOO =~ 'bz.*')";
        String expected = "(FOO =~ 'br.*' || FOO =~ 'bz.*')";
        test(query, expected);

        // with extras
        query = "!(FOO == null) && (FOO =~ 'br.*' || FOO =~ 'bz.*') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO =~ 'br.*' || FOO =~ 'bz.*') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndUnionOfIncludeRegexes() {
        // every field in the union matches the 'not null' term's field, thus we can still prune
        String query = "!(FOO == null) && (filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*'))";
        String expected = "(filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*'))";
        test(query, expected);

        // with extras
        query = "!(FOO == null) && (filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        expected = "(filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testNotNullTermAndUnionOfAllMatches() {
        // every field in the union matches the 'not null' term's field, thus we can still prune
        String query = "!(FOO == null) && (filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*'))";
        String expected = "(filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*'))";
        test(query, expected);

        // with extras
        query = "!(FOO == null) && (filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        expected = "(filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    // test pruning nested union of identical 'is not null' terms

    @Test
    public void testUnionOfNotNullTermsAndUnionOfEqs() {
        // prune via single term
        String query = "(!(FOO == null) || !(FOO == null)) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);

        // prune via union of same field
        query = "(!(FOO == null) || !(FOO == null)) && (FOO == 'bar' || FOO == 'baz')";
        expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);

        // prune via union of same field, extra terms not affected
        query = "(!(FOO == null) || !(FOO == null)) && (FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testUnionOfNotNullTermsAndUnionOfErs() {
        // prune via single term
        String query = "(!(FOO == null) || !(FOO == null)) && FOO =~ 'br.*'";
        String expected = "FOO =~ 'br.*'";
        test(query, expected);

        // prune via union of same field
        query = "(!(FOO == null) || !(FOO == null)) && (FOO =~ 'br.*' || FOO =~ 'bz.*')";
        expected = "(FOO =~ 'br.*' || FOO =~ 'bz.*')";
        test(query, expected);

        // prune via union of same field, extra terms not affected
        query = "(!(FOO == null) || !(FOO == null)) && (FOO =~ 'br.*' || FOO =~ 'bz.*') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO =~ 'br.*' || FOO =~ 'bz.*') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testUnionOfNotNullTermsAndUnionOfIncludeRegexes() {
        // prune via single term
        String query = "(!(FOO == null) || !(FOO == null)) && filter:includeRegex(FOO, 'br.*')";
        String expected = "filter:includeRegex(FOO, 'br.*')";
        test(query, expected);

        // prune via union of same field
        query = "(!(FOO == null) || !(FOO == null)) && (filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*'))";
        expected = "(filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*'))";
        test(query, expected);

        // prune via union of same field, extra terms not affected
        query = "(!(FOO == null) || !(FOO == null)) && (filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        expected = "(filter:includeRegex(FOO, 'br.*') || filter:includeRegex(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    @Test
    public void testUnionOfNotNullTermsAndUnionOfAllMatches() {
        // prune via single term
        String query = "(!(FOO == null) || !(FOO == null)) && filter:getAllMatches(FOO, 'br.*')";
        String expected = "filter:getAllMatches(FOO, 'br.*')";
        test(query, expected);

        // prune via union of same field
        query = "(!(FOO == null) || !(FOO == null)) && (filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*'))";
        expected = "(filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*'))";
        test(query, expected);

        // prune via union of same field, extra terms not affected
        query = "(!(FOO == null) || !(FOO == null)) && (filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        expected = "(filter:getAllMatches(FOO, 'br.*') || filter:getAllMatches(FOO, 'bz.*')) && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
    }

    // test pruning nested union of multiple unique 'is not null' terms

    @Test
    public void testUnionOfUniqueNotNullTermsAndEQs() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && FOO == 'bar' && FOO2 == 'val'";
        String expected = "FOO == 'bar' && FOO2 == 'val'";
        test(query, expected);
    }

    @Test
    public void testUnionOfUniqueNotNullTermsAndERs() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && FOO =~ 'br.*' && FOO2 =~ 'va.*'";
        String expected = "FOO =~ 'br.*' && FOO2 =~ 'va.*'";
        test(query, expected);
    }

    @Test
    public void testUnionOfUniqueNotNullTermsAndIncludeRegexes() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && filter:includeRegex(FOO, 'br.*') && filter:includeRegex(FOO2, 'bz.*')";
        String expected = "filter:includeRegex(FOO, 'br.*') && filter:includeRegex(FOO2, 'bz.*')";
        test(query, expected);
    }

    @Test
    public void testUnionOfUniqueNotNullTermsAndAllMatches() {
        String query = "(!(FOO == null) || !(FOO2 == null)) && filter:getAllMatches(FOO, 'br.*') && filter:getAllMatches(FOO2, 'bz.*')";
        String expected = "filter:getAllMatches(FOO, 'br.*') && filter:getAllMatches(FOO2, 'bz.*')";
        test(query, expected);
    }

    @Test
    public void testUnionOfUniqueNotNullTermsAndMixedOtherTerms() {
        // EQ and ER
        String query = "(!(FOO == null) || !(FOO2 == null)) && FOO == 'bar' && FOO2 =~ 'va.*'";
        String expected = "FOO == 'bar' && FOO2 =~ 'va.*'";
        test(query, expected);

        // EQ and #INCLUDE
        query = "(!(FOO == null) || !(FOO2 == null)) && FOO == 'bar' && FOO2 =~ 'va.*'";
        expected = "FOO == 'bar' && FOO2 =~ 'va.*'";
        test(query, expected);

        // ER and #INCLUDE
        query = "(!(FOO == null) || !(FOO2 == null)) && FOO =~ 'ba.*' && filter:includeRegex(FOO2, 'bz.*')";
        expected = "FOO =~ 'ba.*' && filter:includeRegex(FOO2, 'bz.*')";
        test(query, expected);
    }

    // prune union of multiple unique 'is not null' terms via unions of mixed terms

    @Test
    public void testUnionOfUniqueNotNullTermsAndUnionsOfMixedTerms() {
        // not null && (EQ || ER) && (ER && #INCLUDE)
        String query = "(!(FOO == null) || !(FOO2 == null)) && (FOO == 'bar' && FOO =~ 'ba.*') && (FOO2 =~ 'xy.*' || filter:includeRegex(FOO2, 'yz.*'))";
        String expected = "(FOO == 'bar' && FOO =~ 'ba.*') && (FOO2 =~ 'xy.*' || filter:includeRegex(FOO2, 'yz.*'))";
        test(query, expected);
    }

    // future case

    @Test
    public void testFutureCase_PruneByUnions() {
        // logically, these unions are equivalent and the 'is not null' side can be pruned
        String query = "(!(FOO == null) || !(FOO2 == null)) && (FOO == 'bar' || FOO2 == 'baz')";
        // String expected = "FOO == 'bar' || FOO2 == 'baz'";
        test(query, query);

        // This would be a no-op case
        // String query = "(!(FOO == null) || !(FOO2 == null)) && (FOO == 'bar' || FOO2 == 'baz' || FOO3 == 'buzz')";
    }

    @Test
    public void testFutureCase_FieldForUnion() {
        // in this case FOO is a common field for the nested union and we can prune the isNotNull term
        String query = "!(FOO == null) && (FOO == 'bar' || filter:includeRegex(FOO, 'ba.*'))";
        String expected = "(FOO == 'bar' || filter:includeRegex(FOO, 'ba.*'))";
        // test(query, expected);

        // TODO -- update to f:includeText when #1534 is merged
        query = "!(FOO == null) && (FOO == 'bar' || filter:includeText(FOO, 'ba.*'))";
        // String expected = "(FOO == 'bar' || filter:includeText(FOO, 'ba.*')";
        test(query, query);

        // test case for geo function
        query = "!(FOO == null) && (FOO == 'bar' || geo:within_bounding_box(FOO, '0_0', '10_10'))";
        // String expected = "(FOO == 'bar' || geo:within_bounding_box(FOO, '0_0', '10_10'))";
        test(query, query);
    }

    @Test
    public void testFutureCase_PartialPruneOfUnionViaUnion() {

        // union of same field should allow us to perform a partial prune
        String query = "(!(FOO == null) || !(FOO2 == null)) && (FOO == 'bar' || FOO == 'baz')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);

        // should also work for filter:includeRegex
        query = "(!(FOO == null) || !(FOO2 == null)) && (filter:includeRegex(FOO, 'bar.*') || filter:includeRegex(FOO, 'baz.*'))";
        expected = "(filter:includeRegex(FOO, 'bar.*') || filter:includeRegex(FOO, 'baz.*'))";
        test(query, expected);
    }

    // test cases where nothing should be done

    @Test
    public void testNoOpCases() {
        String query = "!(FOO == null) || FOO == 'bar'";
        test(query, query);

        query = "!(FOO == null)"; // single IsNotNull term
        test(query, query);

        query = "!(FOO == null) || !(FOO == null)"; // union of repeated IsNotNull term
        test(query, query);

        query = "!(FOO == null) && !(FOO == null)"; // intersection of repeated IsNotNull term
        test(query, query);

        query = "!(FOO == null) || !(FOO2 == null)"; // union of different IsNotNull terms
        test(query, query);

        query = "!(FOO == null) && !(FOO2 == null)"; // intersection of different IsNotNull terms
        test(query, query);

        query = "!(FOO == null) && (FOO == 'bar' || FOO2 == 'baz')";
        test(query, query);

        // union of same field, contains invalid node type (NR)
        query = "(!(FOO == null) || !(FOO == null)) && (FOO == 'bar' || FOO !~ 'baz.*') && (FEE == 'fi' || FO == 'fum')";
        test(query, query);

        // an equality node exists and a non-matching isNotNull function is in an adjacent union
        query = "FOO == 'bar' && (FOO2 == 'aa' || !(FOO3 == null) || FOO4 == 'bb')";
        test(query, query);

        // cannot prune half of a union
        query = "(!(FOO == null) || !(FOO2 == null)) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);

        query = "(!(FOO == null) || !(FOO2 == null)) && FOO =~ 'ba.*'";
        expected = "FOO =~ 'ba.*'";
        test(query, expected);
    }

    @Test
    public void testNoOpMultiFieldedIncludeRegex() {
        String query = "!(FOO == null) && filter:includeRegex((FOO||FOO2||FOO3), 'ba.*')";
        test(query, query);
    }

    @Test
    public void testNoOpMultiFieldedGetMatches() {
        String query = "!(FOO == null) && filter:getAllMatches((FOO||FOO2||FOO3), 'ba.*')";
        test(query, query);
    }

    // code that handles producing a flattened query tree with respect to wrapped single terms
    // should not modify marked nodes
    @Test
    public void testNoOpQueryPropertyMarkers() {
        String query = "((_Delayed_ = true) && (!(FOO == null) && FOO == 'bar'))";
        test(query, query);

        query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*r'))";
        test(query, query);

        query = "((_List_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))";
        test(query, query);

        query = "((_Term_ = true) && (FOO == 'bar'))";
        test(query, query);

        query = "((_Delayed_ = true) && (!(F1 == 'v1') || !((_Term_ = true) && (F2 == 'v2'))))";
        test(query, query);
    }

    @Test
    public void testPruningNestedUnionOfIsNotNullFunctions() {
        // logically, these unions are equivalent and the 'is not null' side can be pruned
        String query = "FOO == 'bar' && (!(FOO == null) || !(FOO2 == null) || !(FOO3 == null) || !(FOO4 == null))";
        String expected = "FOO == 'bar'";

        test(query, expected);
    }

    @Test
    public void testPruningNestedUnionOfIsNotNullFunctions_Two() {
        // in this case, since the FOO field is not in the union nothing will be pruned.
        String query = "FOO == 'bar' && (!(FOO2 == null) || !(FOO4 == null))";
        test(query, query);
    }

    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript visited = (ASTJexlScript) IsNotNullPruningVisitor.prune(script);
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);

            assertTrue("visit produced an invalid tree", validator.isValid(visited));
            assertTrue(JexlStringBuildingVisitor.buildQueryWithoutParse(visited), TreeEqualityVisitor.checkEquality(visited, expectedScript).isEqual());

        } catch (ParseException | InvalidQueryTreeException e) {
            e.printStackTrace();
            fail("Failed to parse or validate query: " + query);
        }
    }

}
