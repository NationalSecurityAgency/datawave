package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class RewriteRegexVisitorTest {

    private final Set<String> indexedFields = Set.of("F", "F2", "IO", "IO2");
    private final Set<String> indexOnlyFields = Set.of("IO", "IO2");

    private final Set<String> includeFields = new HashSet<>();
    private final Set<String> excludeFields = new HashSet<>();

    private final Set<RegexRewritePattern> patterns = new HashSet<>();

    @BeforeEach
    public void beforeEach() {
        includeFields.clear();
        excludeFields.clear();
        patterns.clear();
    }

    // A and regex
    @Test
    public void testSingleTermAndRegex() {
        // term and indexed regex
        test("F == 'a' && F =~ 'ba.*'", "F == 'a' && filter:includeRegex(F, 'ba.*')");
        test("IO == 'a' && F =~ 'ba.*'", "IO == 'a' && filter:includeRegex(F, 'ba.*')");
        test("NA == 'a' && F =~ 'ba.*'");

        // term and index only regex is never rewritten
        test("F == 'a' && IO =~ 'ba.*'");
        test("IO == 'a' && IO =~ 'ba.*'");
        test("NA == 'a' && IO =~ 'ba.*'");

        // term and non-indexed regex is always rewritten
        test("F == 'a' && NA =~ 'ba.*'", "F == 'a' && filter:includeRegex(NA, 'ba.*')");
        test("IO == 'a' && NA =~ 'ba.*'", "IO == 'a' && filter:includeRegex(NA, 'ba.*')");
        test("NA == 'a' && NA =~ 'ba.*'", "NA == 'a' && filter:includeRegex(NA, 'ba.*')");
    }

    // A or regex
    @Test
    public void testSingleTermOrRegex() {
        // term or indexed regex is never rewritten
        test("F == 'a' || F =~ 'ba.*'");
        test("IO == 'a' || F =~ 'ba.*'");
        test("NA == 'a' || F =~ 'ba.*'");

        // term or index only regex is never rewritten
        test("F == 'a' || IO =~ 'ba.*'");
        test("IO == 'a' || IO =~ 'ba.*'");
        test("NA == 'a' || IO =~ 'ba.*'");

        // top level union with non-indexed regex is a full table scan, do not rewrite
        test("F == 'a' || NA =~ 'ba.*'", "F == 'a' || filter:includeRegex(NA, 'ba.*')");
        test("IO == 'a' || NA =~ 'ba.*'", "IO == 'a' || filter:includeRegex(NA, 'ba.*')");
        test("NA == 'a' || NA =~ 'ba.*'", "NA == 'a' || filter:includeRegex(NA, 'ba.*')");
    }

    // (A and B) or regex
    @Test
    public void testNestedIntersectionOrRegex() {
        // all combinations of nested intersection and indexed regex
        test("(F == 'a' && F == 'b') || F =~ 'ba.*'");
        test("(F == 'a' && IO == 'b') || F =~ 'ba.*'");
        test("(F == 'a' && NA == 'b') || F =~ 'ba.*'");
        test("(IO == 'a' && IO == 'b') || F =~ 'ba.*'");
        test("(IO == 'a' && NA == 'b') || F =~ 'ba.*'");
        test("(NA == 'a' && NA == 'b') || F =~ 'ba.*'");

        // all combinations of nested intersection and index only regex
        test("(F == 'a' && F == 'b') || IO =~ 'ba.*'");
        test("(F == 'a' && IO == 'b') || IO =~ 'ba.*'");
        test("(F == 'a' && NA == 'b') || IO =~ 'ba.*'");
        test("(IO == 'a' && IO == 'b') || IO =~ 'ba.*'");
        test("(IO == 'a' && NA == 'b') || IO =~ 'ba.*'");
        test("(NA == 'a' && NA == 'b') || IO =~ 'ba.*'");

        // the input queries are non-executable, non-indexed field still gets rewritten
        // all combinations of nested intersection and non-indexed regex
        test("(F == 'a' && F == 'b') || NA =~ 'ba.*'", "(F == 'a' && F == 'b') || filter:includeRegex(NA, 'ba.*')");
        test("(F == 'a' && IO == 'b') || NA =~ 'ba.*'", "(F == 'a' && IO == 'b') || filter:includeRegex(NA, 'ba.*')");
        test("(F == 'a' && NA == 'b') || NA =~ 'ba.*'", "(F == 'a' && NA == 'b') || filter:includeRegex(NA, 'ba.*')");
        test("(IO == 'a' && IO == 'b') || NA =~ 'ba.*'", "(IO == 'a' && IO == 'b') || filter:includeRegex(NA, 'ba.*')");
        test("(IO == 'a' && NA == 'b') || NA =~ 'ba.*'", "(IO == 'a' && NA == 'b') || filter:includeRegex(NA, 'ba.*')");
        test("(NA == 'a' && NA == 'b') || Na =~ 'ba.*'", "(NA == 'a' && NA == 'b') || filter:includeRegex(Na, 'ba.*')");
    }

    // (A or B) and regex
    @Test
    public void testNestedUnionAndRegex() {
        // all combinations of nested intersection and indexed regex
        test("(F == 'a' || F == 'b') && F =~ 'ba.*'", "(F == 'a' || F == 'b') && filter:includeRegex(F, 'ba.*')");
        test("(F == 'a' || IO == 'b') && F =~ 'ba.*'", "(F == 'a' || IO == 'b') && filter:includeRegex(F, 'ba.*')");
        test("(F == 'a' || NA == 'b') && F =~ 'ba.*'");
        test("(IO == 'a' || IO == 'b') && F =~ 'ba.*'", "(IO == 'a' || IO == 'b') && filter:includeRegex(F, 'ba.*')");
        test("(IO == 'a' || NA == 'b') && F =~ 'ba.*'");
        test("(NA == 'a' || NA == 'b') && F =~ 'ba.*'");

        // all combinations of nested intersection and index only regex
        test("(F == 'a' || F == 'b') && IO =~ 'ba.*'");
        test("(F == 'a' || IO == 'b') && IO =~ 'ba.*'");
        test("(F == 'a' || NA == 'b') && IO =~ 'ba.*'");
        test("(IO == 'a' || IO == 'b') && IO =~ 'ba.*'");
        test("(IO == 'a' || NA == 'b') && IO =~ 'ba.*'");
        test("(NA == 'a' || NA == 'b') && IO =~ 'ba.*'");

        // all combinations of nested intersection and non-indexed regex
        test("(F == 'a' || F == 'b') && NA =~ 'ba.*'", "(F == 'a' || F == 'b') && filter:includeRegex(NA, 'ba.*')");
        test("(F == 'a' || IO == 'b') && NA =~ 'ba.*'", "(F == 'a' || IO == 'b') && filter:includeRegex(NA, 'ba.*')");
        test("(F == 'a' || NA == 'b') && NA =~ 'ba.*'", "(F == 'a' || NA == 'b') && filter:includeRegex(NA, 'ba.*')");
        test("(IO == 'a' || IO == 'b') && NA =~ 'ba.*'", "(IO == 'a' || IO == 'b') && filter:includeRegex(NA, 'ba.*')");
        test("(IO == 'a' || NA == 'b') && NA =~ 'ba.*'", "(IO == 'a' || NA == 'b') && filter:includeRegex(NA, 'ba.*')");
        test("(NA == 'a' || NA == 'b') && Na =~ 'ba.*'", "(NA == 'a' || NA == 'b') && filter:includeRegex(Na, 'ba.*')");
    }

    // A and (B or regex)
    @Test
    public void testIntersectionWithNestedUnionWithSingleRegex() {
        // top level indexed term, variable indexed state for nested term, indexed regex
        test("F == 'a' && (F == 'b' || F =~ 'ba.*')", "F == 'a' && (F == 'b' || filter:includeRegex(F, 'ba.*'))");
        test("F == 'a' && (IO == 'b' || F =~ 'ba.*')", "F == 'a' && (IO == 'b' || filter:includeRegex(F, 'ba.*'))");
        test("F == 'a' && (NA == 'b' || F =~ 'ba.*')", "F == 'a' && (NA == 'b' || filter:includeRegex(F, 'ba.*'))");

        // top level indexed term, variable indexed state for nested term, index only regex
        test("F == 'a' && (F == 'b' || IO =~ 'ba.*')");
        test("F == 'a' && (IO == 'b' || IO =~ 'ba.*')");
        test("F == 'a' && (NA == 'b' || IO =~ 'ba.*')");

        // top level indexed term, variable indexed state for nested term, non-indexed regex
        test("F == 'a' && (F == 'b' || NA =~ 'ba.*')", "F == 'a' && (F == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("F == 'a' && (IO == 'b' || NA =~ 'ba.*')", "F == 'a' && (IO == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("F == 'a' && (NA == 'b' || NA =~ 'ba.*')", "F == 'a' && (NA == 'b' || filter:includeRegex(NA, 'ba.*'))");

        // top level index only term, variable indexed state for nested term, indexed regex
        test("IO == 'a' && (F == 'b' || F =~ 'ba.*')", "IO == 'a' && (F == 'b' || filter:includeRegex(F, 'ba.*'))");
        test("IO == 'a' && (IO == 'b' || F =~ 'ba.*')", "IO == 'a' && (IO == 'b' || filter:includeRegex(F, 'ba.*'))");
        test("IO == 'a' && (NA == 'b' || F =~ 'ba.*')", "IO == 'a' && (NA == 'b' || filter:includeRegex(F, 'ba.*'))");

        // top level index only term, variable indexed state for nested term, index only regex
        test("IO == 'a' && (F == 'b' || IO =~ 'ba.*')");
        test("IO == 'a' && (IO == 'b' || IO =~ 'ba.*')");
        test("IO == 'a' && (NA == 'b' || IO =~ 'ba.*')");

        // top level index only term, variable indexed state for nested term, non-indexed regex
        test("IO == 'a' && (F == 'b' || NA =~ 'ba.*')", "IO == 'a' && (F == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("IO == 'a' && (IO == 'b' || NA =~ 'ba.*')", "IO == 'a' && (IO == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("IO == 'a' && (NA == 'b' || NA =~ 'ba.*')", "IO == 'a' && (NA == 'b' || filter:includeRegex(NA, 'ba.*'))");

        // top level non-indexed term, variable indexed state for nested term, indexed regex
        test("NA == 'a' && (F == 'b' || F =~ 'ba.*')");
        test("NA == 'a' && (IO == 'b' || F =~ 'ba.*')");
        test("NA == 'a' && (NA == 'b' || F =~ 'ba.*')");

        // top level non-indexed term, variable indexed state for nested term, index only regex
        test("NA == 'a' && (F == 'b' || IO =~ 'ba.*')");
        test("NA == 'a' && (IO == 'b' || IO =~ 'ba.*')");
        test("NA == 'a' && (NA == 'b' || IO =~ 'ba.*')");

        // top level non-indexed term, variable indexed state for nested term, non-indexed regex
        test("NA == 'a' && (F == 'b' || NA =~ 'ba.*')", "NA == 'a' && (F == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("NA == 'a' && (IO == 'b' || NA =~ 'ba.*')", "NA == 'a' && (IO == 'b' || filter:includeRegex(NA, 'ba.*'))");
        test("NA == 'a' && (NA == 'b' || NA =~ 'ba.*')", "NA == 'a' && (NA == 'b' || filter:includeRegex(NA, 'ba.*'))");
    }

    // A or (B and regex)
    @Test
    public void testUnionWithNestedIntersectionWithSingleRegex() {
        // top level indexed, variable index state of nested term, indexed regex
        test("F == 'a' || (F == 'b' && F == 'ab.*')");
        test("F == 'a' || (IO == 'b' && F == 'ab.*')");
        test("F == 'a' || (NA == 'b' && F == 'ab.*')");

        // top level indexed, variable index state of nested term, index only regex
        test("F == 'a' || (F == 'b' && IO == 'ab.*')");
        test("F == 'a' || (IO == 'b' && IO == 'ab.*')");
        test("F == 'a' || (NA == 'b' && IO == 'ab.*')");

        // top level indexed, variable index state of nested term, non-indexed regex
        test("F == 'a' || (F == 'b' && NA == 'ab.*')");
        test("F == 'a' || (IO == 'b' && NA == 'ab.*')");
        test("F == 'a' || (NA == 'b' && NA == 'ab.*')");

        // top level index only, variable index state of nested term, indexed regex
        test("IO == 'a' || (F == 'b' && F == 'ab.*')");
        test("IO == 'a' || (IO == 'b' && F == 'ab.*')");
        test("IO == 'a' || (NA == 'b' && F == 'ab.*')");

        // top level index only, variable index state of nested term, index only regex
        test("IO == 'a' || (F == 'b' && IO == 'ab.*')");
        test("IO == 'a' || (IO == 'b' && IO == 'ab.*')");
        test("IO == 'a' || (NA == 'b' && IO == 'ab.*')");

        // top level index only, variable index state of nested term, non-indexed regex
        test("IO == 'a' || (F == 'b' && NA == 'ab.*')");
        test("IO == 'a' || (IO == 'b' && NA == 'ab.*')");
        test("IO == 'a' || (NA == 'b' && NA == 'ab.*')");

        // top level non-indexed, variable index state of nested term, indexed regex
        test("NA == 'a' || (F == 'b' && F == 'ab.*')");
        test("NA == 'a' || (IO == 'b' && F == 'ab.*')");
        test("NA == 'a' || (NA == 'b' && F == 'ab.*')");

        // top level non-indexed, variable index state of nested term, index only regex
        test("NA == 'a' || (F == 'b' && IO == 'ab.*')");
        test("NA == 'a' || (IO == 'b' && IO == 'ab.*')");
        test("NA == 'a' || (NA == 'b' && IO == 'ab.*')");

        // top level non-indexed, variable index state of nested term, non-indexed regex
        test("NA == 'a' || (F == 'b' && NA == 'ab.*')");
        test("NA == 'a' || (IO == 'b' && NA == 'ab.*')");
        test("NA == 'a' || (NA == 'b' && NA == 'ab.*')");
    }

    // A and (regex or regex)
    @Test
    public void testIntersectionWithNestedUnionOfRegexes() {
        // indexed term and union of regexes with all possible index states
        test("F == 'a' && (F =~ 'ab.*' || F =~ 'ac.*')", "F == 'a' && (filter:includeRegex(F, 'ab.*') || filter:includeRegex(F, 'ac.*'))");
        test("F == 'a' && (F =~ 'ab.*' || IO =~ 'ac.*')", "F == 'a' && (filter:includeRegex(F, 'ab.*') || IO =~ 'ac.*')");
        test("F == 'a' && (F =~ 'ab.*' || NA =~ 'ac.*')", "F == 'a' && (filter:includeRegex(F, 'ab.*') || filter:includeRegex(NA, 'ac.*'))");
        test("F == 'a' && (IO =~ 'ab.*' || IO =~ 'ac.*')");
        test("F == 'a' && (IO =~ 'ab.*' || NA =~ 'ac.*')", "F == 'a' && (IO =~ 'ab.*' || filter:includeRegex(NA, 'ac.*'))");
        test("F == 'a' && (NA =~ 'ab.*' || NA =~ 'ac.*')", "F == 'a' && (filter:includeRegex(NA, 'ab.*') || filter:includeRegex(NA, 'ac.*'))");

        // index only term and union of regexes with all possible index states
        test("IO == 'a' && (F =~ 'ab.*' || F =~ 'ac.*')", "IO == 'a' && (filter:includeRegex(F, 'ab.*') || filter:includeRegex(F, 'ac.*'))");
        test("IO == 'a' && (F =~ 'ab.*' || IO =~ 'ac.*')", "IO == 'a' && (filter:includeRegex(F, 'ab.*') || IO =~ 'ac.*')");
        test("IO == 'a' && (F =~ 'ab.*' || NA =~ 'ac.*')", "IO == 'a' && (filter:includeRegex(F, 'ab.*') || filter:includeRegex(NA, 'ac.*'))");
        test("IO == 'a' && (IO =~ 'ab.*' || IO =~ 'ac.*')");
        test("IO == 'a' && (IO =~ 'ab.*' || NA =~ 'ac.*')", "IO == 'a' && (IO =~ 'ab.*' || filter:includeRegex(NA, 'ac.*'))");
        test("IO == 'a' && (NA =~ 'ab.*' || NA =~ 'ac.*')", "IO == 'a' && (filter:includeRegex(NA, 'ab.*') || filter:includeRegex(NA, 'ac.*'))");

        // non-indexed tem and union of regexes with all possible index states
        test("NA == 'a' && (F =~ 'ab.*' || F =~ 'ac.*')");
        test("NA == 'a' && (F =~ 'ab.*' || IO =~ 'ac.*')");
        test("NA == 'a' && (F =~ 'ab.*' || NA =~ 'ac.*')", "NA == 'a' && (F =~ 'ab.*' || filter:includeRegex(NA, 'ac.*'))");
        test("NA == 'a' && (IO =~ 'ab.*' || IO =~ 'ac.*')");
        test("NA == 'a' && (IO =~ 'ab.*' || NA =~ 'ac.*')", "NA == 'a' && (IO =~ 'ab.*' || filter:includeRegex(NA, 'ac.*'))");
        test("NA == 'a' && (NA =~ 'ab.*' || NA =~ 'ac.*')", "NA == 'a' && (filter:includeRegex(NA, 'ab.*') || filter:includeRegex(NA, 'ac.*'))");
    }

    // A or (regex and regex)
    @Test
    public void testUnionWithNestedIntersectionOfRegexes() {
        // indexed term or intersection of regexes with all possible index states
        test("F == 'a' || (F =~ 'ab.*' && F =~ 'ac.*')", "F == 'a' || (filter:includeRegex(F, 'ab.*') && F =~ 'ac.*')");
        test("F == 'a' || (F =~ 'ab.*' && IO =~ 'ac.*')", "F == 'a' || (filter:includeRegex(F, 'ab.*') && IO =~ 'ac.*')");
        test("F == 'a' || (F =~ 'ab.*' && NA =~ 'ac.*')", "F == 'a' || (F =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("F == 'a' || (IO =~ 'ab.*' && IO =~ 'ac.*')");
        test("F == 'a' || (IO =~ 'ab.*' && NA =~ 'ac.*')", "F == 'a' || (IO =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("F == 'a' || (NA =~ 'ab.*' && NA =~ 'ac.*')", "F == 'a' || (filter:includeRegex(NA, 'ab.*') && filter:includeRegex(NA, 'ac.*'))");

        // index only term or intersection of regexes with all possible index states
        test("IO == 'a' || (F =~ 'ab.*' && F =~ 'ac.*')", "IO == 'a' || (filter:includeRegex(F, 'ab.*') && F =~ 'ac.*')");
        test("IO == 'a' || (F =~ 'ab.*' && IO =~ 'ac.*')", "IO == 'a' || (filter:includeRegex(F, 'ab.*') && IO =~ 'ac.*')");
        test("IO == 'a' || (F =~ 'ab.*' && NA =~ 'ac.*')", "IO == 'a' || (F =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("IO == 'a' || (IO =~ 'ab.*' && IO =~ 'ac.*')");
        test("IO == 'a' || (IO =~ 'ab.*' && NA =~ 'ac.*')", "IO == 'a' || (IO =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("IO == 'a' || (NA =~ 'ab.*' && NA =~ 'ac.*')", "IO == 'a' || (filter:includeRegex(NA, 'ab.*') && filter:includeRegex(NA, 'ac.*'))");

        // non-indexed tem or intersection of regexes with all possible index states
        test("NA == 'a' || (F =~ 'ab.*' && F =~ 'ac.*')", "NA == 'a' || (filter:includeRegex(F, 'ab.*') && F =~ 'ac.*')");
        test("NA == 'a' || (F =~ 'ab.*' && IO =~ 'ac.*')", "NA == 'a' || (filter:includeRegex(F, 'ab.*') && IO =~ 'ac.*')");
        test("NA == 'a' || (F =~ 'ab.*' && NA =~ 'ac.*')", "NA == 'a' || (F =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("NA == 'a' || (IO =~ 'ab.*' && IO =~ 'ac.*')");
        test("NA == 'a' || (IO =~ 'ab.*' && NA =~ 'ac.*')", "NA == 'a' || (IO =~ 'ab.*' && filter:includeRegex(NA, 'ac.*'))");
        test("NA == 'a' || (NA =~ 'ab.*' && NA =~ 'ac.*')", "NA == 'a' || (filter:includeRegex(NA, 'ab.*') && filter:includeRegex(NA, 'ac.*'))");
    }

    // (A or regex) and (B or regex)
    @Test
    public void testNestedUnionsWithDistributedRegexes() {
        String query = "(F == 'a' || F =~ 'ab.*') && (F == 'b' || F =~ 'ac.*')";
        String expected = "(F == 'a' || filter:includeRegex(F, 'ab.*')) && (F == 'b' || F =~ 'ac.*')";
        test(query, expected);

        query = "(F == 'a' || NA =~ 'ab.*') && (F == 'b' || F =~ 'ac.*')";
        expected = "(F == 'a' || filter:includeRegex(NA, 'ab.*')) && (F == 'b' || F =~ 'ac.*')";
        test(query, expected);
    }

    // (A and regex) or (B and regex)
    @Test
    public void testNestedIntersectionsWithDistributedRegexes() {
        String query = "(F == 'a' && F =~ 'ab.*') || (F == 'b' && F =~ 'ac.*')";
        String expected = "(F == 'a' && filter:includeRegex(F, 'ab.*')) || (F == 'b' && filter:includeRegex(F, 'ac.*'))";
        test(query, expected);
    }

    // (A or B) and (regex or regex)
    @Test
    public void testPartialAnchorAndNestedUnionRegex() {
        String query = "(F == 'a' || F == 'b') && (F =~ 'ab.*' || F =~ 'ac.*')";
        String expected = "(F == 'a' || F == 'b') && (filter:includeRegex(F, 'ab.*') || filter:includeRegex(F, 'ac.*'))";
        test(query, expected);
    }

    // A and (B or (C and regex)
    @Test
    public void testLeftAnchorAndDeeplyNestedRegex() {
        String query = "F == 'a' && (F == 'b' || (F == 'c' && F =~ 'ab.*'))";
        String expected = "F == 'a' && (F == 'b' || (F == 'c' && filter:includeRegex(F, 'ab.*')))";
        test(query, expected);
    }

    // ((regex and C) or B) and A
    @Test
    public void testRightAnchorAndDeeplyNestedRegex() {
        String query = "((F =~ 'ab.*' && F == 'c') || F == 'b') && F == 'a'";
        String expected = "((filter:includeRegex(F, 'ab.*') && F == 'c') || F == 'b') && F == 'a'";
        test(query, expected);
    }

    @Test
    public void testUnionOfTwoLegalRewrites() {
        String query = "(F == 'a' && F =~ 'ab.*') || (F == 'b' && F =~ 'ac.*')";
        String expected = "(F == 'a' && filter:includeRegex(F, 'ab.*')) || (F == 'b' && filter:includeRegex(F, 'ac.*'))";
        test(query, expected);
    }

    // (NA and regex) or (NA and regex)
    @Test
    public void testUnionOfTwoIllegalRewrites() {
        String query = "(NA == 'a' && F =~ 'ab.*') || (NA == 'b' && F =~ 'ac.*')";
        test(query);
    }

    @Test
    public void testIncludeFieldsPreventNoRewrites() {
        withIncludeFields(Set.of("F", "F2"));
        test("IO == 'a' && F =~ 'ab.*' && F2 =~ 'ac.*'", "IO == 'a' && filter:includeRegex(F, 'ab.*') && filter:includeRegex(F2, 'ac.*')");
    }

    @Test
    public void testIncludeFieldsPreventSomeLegalRewrites() {
        withIncludeFields(Set.of("F2"));
        test("IO == 'a' && F =~ 'ab.*' && F2 =~ 'ac.*'", "IO == 'a' && F =~ 'ab.*' && filter:includeRegex(F2, 'ac.*')");
    }

    @Test
    public void testExcludeFieldsPreventAllLegalRewrites() {
        withExcludeFields(Set.of("F", "F2"));
        test("IO == 'a' && F =~ 'ab.*' && F2 =~ 'ac.*'");
    }

    @Test
    public void testExcludeFieldsPreventSomeLegalRewrites() {
        withExcludeFields(Set.of("F2"));
        test("IO == 'a' && F =~ 'ab.*' && F2 =~ 'ac.*'", "IO == 'a' && filter:includeRegex(F, 'ab.*') && F2 =~ 'ac.*'");
    }

    @Test
    public void testFullyInclusiveIncludeAndExcludeFields() {
        withIncludeFields(Set.of("F"));
        withExcludeFields(Set.of("F"));
        // exclude fields beats include fields
        test("IO == 'a' && F =~ 'ab.*'");
    }

    @Test
    public void testPatternBeatsExcludeFields() {
        withPattern("F", "zz.*");
        withExcludeFields(Set.of("F"));
        // pattern beats exclude fields
        test("IO == 'a' && F =~ 'zz.*'", "IO == 'a' && filter:includeRegex(F, 'zz.*')");
    }

    @Test
    public void testPatternBeatsIncludeFields() {
        withPattern("F", "zz.*");
        withIncludeFields(Set.of("F2"));
        // pattern beats include fields
        test("IO == 'a' && F =~ 'zz.*'", "IO == 'a' && filter:includeRegex(F, 'zz.*')");
    }

    @Test
    public void testPatternBeatsIncludeAndExcludeFields() {
        withPattern("F", "zz.*");
        withIncludeFields(Set.of("F2"));
        withExcludeFields(Set.of("F"));
        // pattern beats include fields
        test("IO == 'a' && F =~ 'zz.*'", "IO == 'a' && filter:includeRegex(F, 'zz.*')");
    }

    /**
     * Assert that the provided query does not change
     *
     * @param query
     *            the query
     */
    private void test(String query) {
        test(query, query);
    }

    /**
     * Assert that the provided query matches the expected query after the {@link RewriteRegexVisitor} is applied
     *
     * @param query
     *            the query
     * @param expected
     *            the expected result
     */
    private void test(String query, String expected) {
        ASTJexlScript script = parse(query);
        RewriteRegexVisitor.rewrite(script, indexedFields, indexOnlyFields, includeFields, excludeFields, patterns);
        String result = JexlStringBuildingVisitor.buildQuery(script);
        assertEquals(expected, result);
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

    private void withIncludeFields(Set<String> includeFields) {
        this.includeFields.addAll(includeFields);
    }

    private void withExcludeFields(Set<String> excludeFields) {
        this.excludeFields.addAll(excludeFields);
    }

    private void withPattern(String field, String literal) {
        patterns.add(new RegexRewritePattern(field, literal));
    }
}
