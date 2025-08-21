package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SystemLimitProviderTests {

    private static final int defaultLimit = 5000;
    private SystemLimitProvider provider;
    protected final List<SystemLimitConfiguration> systemConfigs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        provider = null;
        systemConfigs.clear();
    }

    /**
     * Verify that configurations with blank system patterns are forbidden.
     */
    @Test
    void testConfigWithBlankSystemPattern() {
        givenSystemConfig(" ", 10, true, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("System query limit configuration specified with blank system pattern");
    }

    /**
     * Verify that configurations with regex system patterns that cannot be compiled are forbidden.
     */
    @Test
    void testConfigWithUncompilableSystemPattern() {
        givenSystemConfig("SYS[", 10, true, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid regex in system pattern 'SYS['");
    }

    /**
     * Verify that multiple configurations using the same system patterns are forbidden.
     */
    @Test
    void testMultipleConfigsWithSameSystemPattern() {
        givenSystemConfig("SYSTEM_01*", 10, true, null);
        givenSystemConfig("SYSTEM_01*", 10, true, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Multiple query limit configurations specified with system pattern 'SYSTEM_01*'");
    }

    /**
     * Verify that configurations with conflicting system patterns that are equivalent exact matches are forbidden.
     */
    @Test
    void testEquivalentExactMatchPatterns() {
        givenSystemConfig("SYSTEM_01", 10, true, null); // Literals only.
        givenSystemConfig("SYSTEM\\_01", 10, true, null); // Literals and escaped literals.

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("System pattern 'SYSTEM\\_01' will resolve to an exact match that is equivalent to system pattern 'SYSTEM_01' from "
                                        + "another system configuration.");
    }

    /**
     * Verify that configurations with negative query limits are forbidden.
     */
    @Test
    void testNegativeQueryLimit() {
        givenSystemConfig("SYSTEM_01*", -1, true, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Negative query limit specified for system pattern 'SYSTEM_01*'");
    }

    /**
     * Verify that configurations with an implied wildcard system pattern {@code *} that are configured to not apply to user limits are forbidden.
     */
    @Test
    void testImpliedWildcardSystemPatternThatDoesNotApplyToUserLimit() {
        givenSystemConfig("*", 10, false, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("System pattern '*' is wildcard-only and may not be used to override whether queries count against user limits to false");
    }

    /**
     * Verify that configurations with an explicit wildcard system pattern that are configured to not apply to user limits are forbidden.
     */
    @Test
    void testExplicitWildcardSystemPatternThatDoesNotApplyToUserLimit() {
        givenSystemConfig(".*", 10, false, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("System pattern '.*' is wildcard-only and may not be used to override whether queries count against user limits to false");
    }

    /**
     * Verify that we support {@code *} as a wildcard pattern.
     */
    @Test
    void testImpliedWildcardQueryLogicPattern() {
        givenSystemConfig("*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-01")).isEqualTo(SystemQueryLimit.fromConfig("*", 100, true, null));
    }

    /**
     * Verify that an explicit wildcard pattern returns a limit for any system.
     */
    @Test
    void testExplicitWildcardQueryLogicPattern() {
        givenSystemConfig(".*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-01")).isEqualTo(SystemQueryLimit.fromConfig(".*", 100, true, null));
    }

    /**
     * Verify that when we have a partial match and a wildcard match, that the partial match is returned.
     */
    @Test
    void testPartialMatchOverridesWildcardMatch() {
        givenSystemConfig("SYSTEM-.*", 75, false, null);
        givenSystemConfig(".*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-.*")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-.*", 75, false, null));
    }

    /**
     * Verify that when we have multiple partial matches, that the one with the lowest limit is returned.
     */
    @Test
    void testMultiplePartialMatchesReturnsLowestLimit() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-AB.*", 40, true, null);
        givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
        givenSystemConfig(".*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-ABC.*", 30, true, null));
    }

    /**
     * Verify that when we have an exact match, partial match and a wildcard match, that the exact match is returned.
     */
    @Test
    void testExactMatchOverridesPartialAndWildcardMatch() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-AB.*", 40, true, null);
        givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
        givenSystemConfig("SYSTEM-ABC", 400, true, null);
        givenSystemConfig(".*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM-ABC", 400, true, null));
    }

    /**
     * Verify that a system pattern that consists only of literals and escaped literals is treated as an exact match.
     */
    @Test
    void testExactMatchThatHasEscapedLiterals() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-AB.*", 40, true, null);
        givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
        givenSystemConfig("SYSTEM\\-ABC", 400, true, null);
        givenSystemConfig(".*", 100, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-ABC")).isEqualTo(SystemQueryLimit.fromConfig("SYSTEM\\-ABC", 400, true, null));
    }

    /**
     * Verify that when there is no match, that default limits are returned.
     */
    @Test
    void testNoMatch() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-AB.*", 40, true, null);
        givenSystemConfig("SYSTEM-ABC.*", 30, true, null);
        givenSystemConfig("SYSTEM-ABC", 400, true, null);

        initProvider();

        assertThat(provider.getLimit("SYSTEM-ECHO")).isEqualTo(SystemQueryLimit.fromDefaults("SYSTEM-ECHO", defaultLimit));
    }

    /**
     * Verify that when there is no configured match for a system, that queries count against the user limit for the system.
     */
    @Test
    void testCountsAgainstUserLimitNoMatch() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-ABC", 400, true, null);

        initProvider();

        assertThat(provider.countsAgainstUserLimit("SYSTEM-ECHO")).isTrue();
    }

    /**
     * Verify that an exact match where queries count against the user limits returns true.
     */
    @Test
    void testCountsAgainstUserLimitWithExactMatchWithTrue() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-ABC", 400, true, null);

        initProvider();

        assertThat(provider.countsAgainstUserLimit("SYSTEM-ABC")).isTrue();
    }

    /**
     * Verify that an exact match where queries count against the user limits returns false.
     */
    @Test
    void testCountsAgainstUserLimitWithExactMatchWithFalse() {
        givenSystemConfig("SYSTEM-A.*", 75, true, null);
        givenSystemConfig("SYSTEM-ABC", 400, false, null);

        initProvider();

        assertThat(provider.countsAgainstUserLimit("SYSTEM-ABC")).isFalse();
    }

    /**
     * Verify that when there is no exact match and there are multiple partial matches where at least one counts queries against the user limit, true is
     * returned.
     */
    @Test
    void testCountsAgainstUserLimitWithPartialMatchWithTrue() {
        givenSystemConfig("SYSTEM-A.*", 75, true, null);
        givenSystemConfig("SYSTEM-AB.*", 400, false, null);
        givenSystemConfig("SYSTEM-ABC*", 400, false, null);

        initProvider();

        assertThat(provider.countsAgainstUserLimit("SYSTEM-ABC")).isTrue();
    }

    /**
     * Verify that when there is no exact match and there are multiple partial matches where all do not count queries against the user limit, false is returned.
     */
    @Test
    void testCountsAgainstUserLimitWithAllPartialMatchWithFalse() {
        givenSystemConfig("SYSTEM-A.*", 75, false, null);
        givenSystemConfig("SYSTEM-AB.*", 400, false, null);
        givenSystemConfig("SYSTEM-ABC.*", 400, false, null);

        initProvider();

        assertThat(provider.countsAgainstUserLimit("SYSTEM-ABC")).isFalse();
    }

    private void initProvider() {
        this.provider = new SystemLimitProvider(defaultLimit, systemConfigs);
    }

    private void givenSystemConfig(String systemPattern, Integer queryLimit, Boolean countsAgainstUserLimit, Map<String,Integer> queryLogicGroupLimits) {
        SystemLimitConfiguration config = new SystemLimitConfiguration(systemPattern, countsAgainstUserLimit, queryLimit, queryLogicGroupLimits);
        this.systemConfigs.add(config);
    }
}
