package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;

public class QueryOptionsSwitchTest {

    private final Map<String,String> options = new HashMap<>();
    private ShardQueryConfiguration config;

    @BeforeEach
    public void beforeEach() {
        options.clear();
        config = new ShardQueryConfiguration();
    }

    private void givenOption(String key, String value) {
        options.put(key, value);
    }

    private void applyOptions() {
        QueryOptionsSwitch.apply(options, config);
    }

    @Test
    public void testIncludeGroupingContext() {
        assertFalse(config.getIncludeGroupingContext());
        givenOption(QueryParameters.INCLUDE_GROUPING_CONTEXT, Boolean.TRUE.toString());
        applyOptions();
        assertTrue(config.getIncludeGroupingContext());
    }

    @Test
    public void testDsEnabled() {
        assertFalse(config.isUseDocumentScheduler());
        givenOption(QueryParameters.DS_ENABLED, Boolean.TRUE.toString());
        applyOptions();
        assertTrue(config.isUseDocumentScheduler());
    }

}
