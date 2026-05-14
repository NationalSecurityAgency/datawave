package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UserLimitProviderTest {

    private static final int defaultLimit = 100;
    private static final int maxCacheSize = 200;
    private UserLimitProvider provider;
    private final List<UserLimitConfiguration> userConfigs = new ArrayList<>();
    private final List<QueryLogicGroupLimitConfiguration> groupConfigs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        provider = null;
        userConfigs.clear();
        groupConfigs.clear();
    }

    /**
     * Verify that blank user DNs are forbidden.
     */
    @Test
    void testConfigWithBlankDn() {
        givenUserConfig("  ", null, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("User query limit configuration given with blank user DN");
    }

    /**
     * Verify that multiple configurations with the same user DN are forbidden.
     */
    @Test
    void testMultipleConfigsWithSameUserDn() {
        givenUserConfig("cn=test user, c=us", 100, null);
        givenUserConfig("cn=test user, c=us", 200, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Multiple query limit configurations specified for user 'cn=test user, c=us'");
    }

    /**
     * Verify that negative query limits are forbidden.
     */
    @Test
    void testConfigWithNegativeLimit() {
        givenUserConfig("cn=test user, c=us", -1, null);

        assertThatThrownBy(this::initProvider).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Negative user query limit given for user 'cn=test user, c=us'");
    }

    /**
     * Verify that when a user config is provided that does not override any configurable limits, the user config is not retained.
     */
    @Test
    void testUserConfigThatOverridesNothing() {
        givenUserConfig("cn=test user, c=us", null, null);

        initProvider();

        assertThat(provider.hasCustomLimits("cn=test user, c=us")).isFalse();
    }

    /**
     * Verify that when a user config is provided with an override for the user query limit only, that correct limits are returned.
     */
    @Test
    void testUserThatOverridesDefaultQueryLimitOnly() {
        givenGroupConfig("TLD", "TLDQueryLogic", 20);
        givenUserConfig("cn=test user, c=us", 20, null);

        initProvider();

        assertThat(provider.hasCustomLimits("cn=test user, c=us")).isTrue();
        UserLimits userLimits = provider.getCustomLimits("cn=test user, c=us");
        // The overridden user query limit should be present.
        assertThat(userLimits.getQueryLimit()).isEqualTo(20);
        // There should be no group limit overrides returned.
        assertThat(userLimits.getBestGroupLimits("TLDQUeryLogic")).isEmpty();
    }

    /**
     * Verify that when a user config is provided with an override for the group query limit only, that correct limits are returned.
     */
    @Test
    void testUserThatOverridesGroupLimitsOnly() {
        givenGroupConfig("TLD", "TLDQueryLogic", 20);

        givenUserConfig("cn=test user, c=us", null, Map.of("TL.*", 5));

        initProvider();

        assertThat(provider.hasCustomLimits("cn=test user, c=us")).isTrue();

        UserLimits userLimits = provider.getCustomLimits("cn=test user, c=us");
        assertThat(userLimits.getQueryLimit()).isEqualTo(defaultLimit);
        assertThat(userLimits.getBestGroupLimits("TLDQueryLogic")).isEqualTo(Map.of("TLD", 5));
    }

    /**
     * Verify that when a user config is provided with an override for both the default user query limit and the group query limit, that correct limits are
     * returned.
     */
    @Test
    void testUserThatOverridesAllLimitsOnly() {
        givenGroupConfig("TLD", "TLDQueryLogic", 20);

        givenUserConfig("cn=test user, c=us", 20, Map.of("TL.*", 5));

        initProvider();

        assertThat(provider.hasCustomLimits("cn=test user, c=us")).isTrue();

        UserLimits userLimits = provider.getCustomLimits("cn=test user, c=us");
        assertThat(userLimits.getQueryLimit()).isEqualTo(20);
        assertThat(userLimits.getBestGroupLimits("TLDQueryLogic")).isEqualTo(Map.of("TLD", 5));
    }

    /**
     * Verify that when a user config is provided with an override for multiple groups that match against a query logic, that the overrides are returned.
     */
    @Test
    void testMultipleGroupLimitMatches() {
        givenGroupConfig("A_TLD", "TLDQueryLogic.*", 40);
        givenGroupConfig("B_TLD", "TLDQuery.*", 20);
        givenGroupConfig("C_TLD", "TLD.*", 25);

        // Override the limits for A_TLD and B_TLD.
        givenUserConfig("cn=test user, c=us", 20, Map.of("A.*", 5, "B.*", 50));

        initProvider();

        assertThat(provider.hasCustomLimits("cn=test user, c=us")).isTrue();

        UserLimits userLimits = provider.getCustomLimits("cn=test user, c=us");
        assertThat(userLimits.getQueryLimit()).isEqualTo(20);
        // We should get back the groups A_TLD and B_TLD and their overridden limits. C_TLD should also be returned, but with its original limit.
        assertThat(userLimits.getBestGroupLimits("TLDQueryLogic")).isEqualTo(Map.of("A_TLD", 5, "C_TLD", 25, "B_TLD", 50));
    }

    private void givenUserConfig(String userDn, Integer queryLimit, Map<String,Integer> queryLogicLimits) {
        this.userConfigs.add(new UserLimitConfiguration(userDn, queryLimit, queryLogicLimits));
    }

    private void givenGroupConfig(String groupName, String queryLogicPattern, int queryLimit) {
        this.groupConfigs.add(new QueryLogicGroupLimitConfiguration(groupName, queryLogicPattern, queryLimit));
    }

    private void initProvider() {
        QueryLogicGroupLimitProvider groupProvider = new QueryLogicGroupLimitProvider(maxCacheSize, groupConfigs);
        this.provider = new UserLimitProvider(defaultLimit, maxCacheSize, userConfigs, groupProvider);
    }
}
