package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Contains tests focused on validation of behavior when retrieving limits for users.
 */
class UserLimitProviderTests {

    private static final int defaultLimit = 100;
    private UserLimitProvider provider;
    private final List<UserLimitConfiguration> userConfigs = new ArrayList<>();

    @AfterEach
    void tearDown() {
        this.provider = null;
        this.userConfigs.clear();
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
     * Verify that user configurations that override various aspects of query limits are extracted correctly.
     */
    @Test
    void testUserConfigurations() {
        // A user configuration that overrides the user query limit, but does not override any query logic group limits.
        givenUserConfig("cn=userA, c=us", 200, null);

        // A user configuration that does not override the user query limit, but does override the TLDLogics group limit.
        givenUserConfig("cn=userB, c=us", null, Map.of("TLDLogics", 200));

        // A user configuration that overrides both the user query limit, and the TLDLogics group limit.
        givenUserConfig("cn=userC, c=us", 50, Map.of("TLDLogics", 10));

        initProvider();

        // Assert that a user with no configuration has the default limits.
        assertThat(provider.getLimit("cn=userD, c=us")).isEqualTo(UserQueryLimit.fromDefaults("cn=userD, c=us", defaultLimit));

        // Assert userA.
        assertThat(provider.getLimit("cn=userA, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userA, c=us", 200, Set.of()));

        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLDLogics", 200));
        // Assert userB.
        assertThat(provider.getLimit("cn=userB, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userB, c=us", defaultLimit, groupLimits));

        groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLDLogics", 10));
        // Assert userC.
        assertThat(provider.getLimit("cn=userC, c=us")).isEqualTo(UserQueryLimit.fromConfig("cn=userC, c=us", 50, groupLimits));
    }

    private void initProvider() {
        provider = new UserLimitProvider(defaultLimit, userConfigs);
    }

    private void givenUserConfig(String userDn, Integer queryLimit, Map<String,Integer> queryLogicGroupLimits) {
        UserLimitConfiguration config = new UserLimitConfiguration(userDn, queryLimit, queryLogicGroupLimits);
        this.userConfigs.add(config);
    }
}
