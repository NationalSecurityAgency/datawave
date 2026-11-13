package datawave.query.iterator.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class SummaryCreatorTest {

    @Test
    public void formatsDecodedUnicodePrefix() {
        Map<String,String> content = Map.of("CONTENT", "a\u00e9\u4e2d\ud83d\ude00");
        assertEquals("CONTENT: a\u00e9\u4e2d\ud83d\ude00", new SummaryCreator(List.of("CONTENT"), content).createSummary());
    }

    @Test
    public void formatsShortContent() {
        Map<String,String> content = Map.of("CONTENT", "short");
        assertEquals("CONTENT: short", new SummaryCreator(List.of("CONTENT"), content).createSummary());
    }

    @Test
    public void choosesTheHighestPriorityAvailableView() {
        Map<String,String> content = Map.of("CONTENT1", "first", "CONTENT2", "second");
        assertEquals("CONTENT2: second", new SummaryCreator(List.of("MISSING", "CONTENT2", "CONTENT1"), content).createSummary());
    }

    @Test
    public void wildcardSummaryIncludesOnlyMatchingViews() {
        Map<String,String> content = Map.of("CONTENT1", "first", "CONTENT2", "second", "OTHER", "unmatched");
        String summary = new SummaryCreator(List.of("CONTENT*"), content).createSummary();
        assertEquals(Set.of("CONTENT1: first", "CONTENT2: second"), Set.of(summary.split("\n")));
    }

    @Test
    public void viewNamesCanContainColons() {
        Map<String,String> content = Map.of("CONTENT:TEXT", "prefix");
        assertEquals("CONTENT:TEXT: prefix", new SummaryCreator(List.of("CONTENT:TEXT"), content).createSummary());
    }

    @Test
    public void returnsNullWhenNoViewMatches() {
        assertNull(new SummaryCreator(List.of("MISSING"), Map.of("CONTENT", "prefix")).createSummary());
    }
}
