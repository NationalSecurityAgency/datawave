package datawave.webservice.query.limit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import datawave.query.parser.JavaRegexAnalyzer;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * This class provides method for identifying the query limits to enforce for individual users, systems, and query logic groups. This should be instantiated as
 * a singleton bean.
 */
@Component("queryLimitProvider")
public class QueryLimitProvider {
    
    private static final Logger log = Logger.getLogger(QueryLimitProvider.class);
    private static final String ASTERISK = "*";
    
    // Matches against regex patterns that consist of '*' or any combination that results in a wildcard pattern.
    private static final Pattern wildcardOnlyPattern = Pattern.compile("^\\*$|^(\\.\\*)+$");
    
    private final QueryLimitProviderConfiguration config;
    
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;
    private UserLimitProvider userLimitProvider;
    private SystemLimitProvider systemLimitProvider;
    
    @Autowired
    public QueryLimitProvider(QueryLimitProviderConfiguration config) {
        this.config = config;
    }
    
    /**
     * Validate the configuration and extract the query limits.
     */
    @PostConstruct
    protected void postConstruct() {
        if (config == null) {
            throw new NullPointerException("Configuration must not be null");
        }
        
        if(log.isTraceEnabled()) {
            log.trace("Initializing with config: " + config);
        }
        
        if (config.getDefaultUserQueryLimit() < 1) {
            throw new IllegalArgumentException("Default user query limit must be greater than 0");
        }
        
        if(config.getDefaultSystemQueryLimit() < 1) {
            throw new IllegalArgumentException("Default system query limit must be greater than 0");
        }
        
        this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(config.getQueryLogicGroupConfigs());
        this.userLimitProvider = new UserLimitProvider(config.getUserConfigs(), queryLogicGroupLimitProvider.getGroupNames());
        this.systemLimitProvider = new SystemLimitProvider(config.getSystemConfigs(), queryLogicGroupLimitProvider.getGroupNames());
    }
    
    /**
     * Construct and return a {@link Matcher} based off the given pattern.
     * @param pattern the pattern
     * @return the matcher
     * @throws JavaRegexAnalyzer.JavaRegexParseException if the pattern could not be analyzed
     */
    private static Matcher getMatcher(String pattern) throws JavaRegexAnalyzer.JavaRegexParseException {
        if(wildcardOnlyPattern.matcher(pattern).matches()) {
            return new WildcardMatcher();
        } else {
            // Analyze the regex to determine what, if any, regex constructs are present.
            JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(pattern);
            JavaRegexAnalyzer.RegexPart[] regexParts = analyzer.getRegexParts();
            boolean escapedLiteralsSeen = false;
            boolean regexSeen = false;
            // Determine if the regex contains any escaped literals or non-literal regex constructs.
            for(JavaRegexAnalyzer.RegexPart regexPart : regexParts) {
                JavaRegexAnalyzer.RegexType type = regexPart.getType();
                if(type == JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL) {
                    escapedLiteralsSeen = true;
                } else if(type != JavaRegexAnalyzer.RegexType.LITERAL) {
                    // We have seen a non-literal, and can stop early.
                    regexSeen = true;
                    break;
                }
            }
            // If a non-literal regex construct was seen, use a Pattern matcher that falls into the 'partial-match' bucket.
            if(regexSeen) {
                return new PatternMatcher(Pattern.compile(pattern));
            } else if(escapedLiteralsSeen) {
                // If the pattern consists solely of literals and escaped literals, remove the escaping backslashes and use a string matcher that falls into the
                // 'exact-match' bucket.
                String literal = toUnescapedLiteralString(regexParts);
                return new StringMatcher(literal);
            } else {
                // If the pattern consists only of literals, use a string matcher that falls into the 'exact-match' bucket.
                return new StringMatcher(pattern);
            }
        }
    }
    
    /**
     * Return the given parts from a regex pattern as a simple, non-escaped string.
     * @param regexParts the regex parts
     * @return the simplified string
     */
    private static String toUnescapedLiteralString(JavaRegexAnalyzer.RegexPart[] regexParts) {
        StringBuilder sb = new StringBuilder();
        for(JavaRegexAnalyzer.RegexPart part : regexParts) {
            if(part.getType() == JavaRegexAnalyzer.RegexType.LITERAL) {
                sb.append(part.regex);
            } else if(part.getType() == JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL) {
                sb.append(part.getRegex().charAt(1));
            } else {
                throw new IllegalArgumentException("Regex parts must be of type " + JavaRegexAnalyzer.RegexType.LITERAL + " or " +
                                JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL);
            }
        }
        return sb.toString();
    }
    
    /**
     * Return the default user query limit.
     * @return the default user query limit
     */
    public int getDefaultUserQueryLimit() {
        return config.getDefaultUserQueryLimit();
    }
    
    /**
     * Return the default system query limit.
     * @return the default system query limit
     */
    public int getDefaultSystemQueryLimit() {
        return config.getDefaultSystemQueryLimit();
    }
    
    /**
     * Return the query limits for the given user.
     * @param userDn the user DN
     * @return the user's query limits
     */
    public UserQueryLimit getUserLimit(String userDn) {
        return userLimitProvider.getLimit(userDn);
    }
    
    /**
     * Return the query limits for the given system.
     * @param system the system name
     * @return the system's query limits
     */
    public SystemQueryLimit getSystemLimit(String system) {
        return systemLimitProvider.getLimit(system);
    }
    
    /**
     * Return whether queries count against a user's query limit when submitted to the given system
     * @param system the system name
     * @return true if queries count against a user's query limit on the given system, or false otherwise
     */
    public boolean systemCountsAgainstUserLimit(String system) {
        return systemLimitProvider.countsAgainstUserLimit(system);
    }
    
    /**
     * Return the query limits for the given query logic if one was configured within a matching query logic group.
     * @param queryLogic the query logic
     * @return a populated {@link Optional} if a matching group was found, or an empty one otherwise
     */
    public Optional<QueryLogicGroupLimit> getQueryLogicGroupLimit(String queryLogic) {
        return queryLogicGroupLimitProvider.getLimit(queryLogic);
    }
    
    /**
     * Return the query limits for the given query logic based on the given map of overridden group limits.
     * @param sourceId a user DN or system name
     * @param queryLogic the query logic
     * @param overriddenLimits the map of group names to their overriding limit
     * @return a populated {@link Optional} if a matching overriding group was found, or an empty one otherwise
     */
    public Optional<QueryLogicGroupLimit> getOverriddenQueryLogicGroupLimit(String sourceId, String queryLogic, Map<String, Integer> overriddenLimits) {
        return queryLogicGroupLimitProvider.getOverriddenLimit(sourceId, queryLogic, overriddenLimits);
    }
    
    /**
     * This class is responsible for extracting the limits from {@link QueryLogicGroupConfiguration} instances and then identifying the query limits to enforce
     * for individual query logics, overridden or otherwise.
     */
    private static class QueryLogicGroupLimitProvider {
        
        // Cache of query logic keys to either their best configured limits or empty optionals when they have no matching limit.
        private final Cache<String, Optional<QueryLogicGroupLimit>> limitCache = Caffeine.newBuilder().build();
        
        // Cache of (userDn|systemName) + query logic keys to their best overriding limit, or empty optionals when they have no match.
        private final Cache<String, Optional<QueryLogicGroupLimit>> overiddenLimitCache = Caffeine.newBuilder().build();
        
        // The set of unique query logic group names.
        private Set<String> groupNames;
        // Map of group names to their corresponding query logic matcher.
        private Map<String, Matcher> groupMatchers;
        // The set of query limits in sorted order of best match to worst.
        private SortedSet<MatchableLimit> configuredLimits;
    
        private QueryLogicGroupLimitProvider(Collection<QueryLogicGroupConfiguration> configs) {
            if(configs != null && !configs.isEmpty()) {
                validateConfigs(configs);
                populateLimits(configs);
            } else {
                groupNames = Set.of();
                configuredLimits = Collections.emptySortedSet();
            }
        }
        
        /**
         * Validate the given configurations.
         * @param configs the configurations to validate
         */
        private void validateConfigs(Collection<QueryLogicGroupConfiguration> configs) {
            Set<String> groupNames = new HashSet<>();
            for(QueryLogicGroupConfiguration config : configs) {
                
                // Verify that a group name was given.
                String groupName = config.getGroupName();
                if(StringUtils.isBlank(groupName)) {
                    throw new IllegalArgumentException("Query logic group limit configuration given with blank group name");
                }
                
                // Verify that we have not seen a configuration with the group name before.
                if(groupNames.contains(groupName)) {
                    throw new IllegalArgumentException("Multiple query logic group configurations given with group name '" + groupName + "'");
                } else {
                    groupNames.add(groupName);
                }
                
                // Verify that the query limit is not negative.
                if (config.getQueryLimit() < 0) {
                    throw new IllegalArgumentException("Negative limit given for query logic group '" + groupName + "'");
                }
                
                // Verify that a query logic pattern was given.
                String queryLogicPattern = config.getQueryLogicPattern();
                if (StringUtils.isBlank(queryLogicPattern)) {
                    throw new IllegalArgumentException("Blank query logic pattern given for query logic group '" + groupName + "'");
                }
                
                // Verify that the pattern compiles if it is not simply a * as is occasionally used as a wildcard in configurations.
                try {
                    if (!queryLogicPattern.equals(ASTERISK)) {
                        Pattern.compile(queryLogicPattern);
                    }
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regex in query logic pattern '" + queryLogicPattern + "' for query logic group '" + groupName + "'", e);
                }
            }
            this.groupNames = Set.copyOf(groupNames);
        }
        
        /**
         * Populate the limits to enforce for query logic groups.
         * @param configs the configs to populate the limits from
         */
        private void populateLimits(Collection<QueryLogicGroupConfiguration> configs) {
            Map<String,Matcher> groupMatchers = new HashMap<>();
            SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
            // Create a matchable limit for each configuration.
            for(QueryLogicGroupConfiguration config : configs) {
                
                // Identify the best matching strategy to use when matching query logics.
                String queryLogicPattern = config.getQueryLogicPattern();
                Matcher matcher;
                try {
                    matcher = getMatcher(queryLogicPattern);
                } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                    throw new IllegalArgumentException("Failed to analyze regex for query logic pattern '" + queryLogicPattern + "' for query logic group '" + config.getGroupName() + "': " + e.getMessage(), e);
                }
                
                // Add a new matchable limit. The sorted set will be sorted in the following priority:
                // 1. First by matching type: EXACT, then PARTIAL, then ALL
                // 2. Then by query limit, from lowest to highest.
                // 3. Then by group name.
                groupLimits.add(new MatchableLimit(config.getGroupName(), matcher, config.getQueryLimit()));
                groupMatchers.put(config.getGroupName(), matcher);
            }
            
            this.configuredLimits = Collections.unmodifiableSortedSet(groupLimits);
            this.groupMatchers = Map.copyOf(groupMatchers);
        }
        
        /**
         * Return the set of all query logic groups that this provider was configured with.
         * @return the query logic group names
         */
        private Set<String> getGroupNames() {
            return groupNames;
        }
        
        /**
         * Return the best matching {@link QueryLogicGroupLimit} to use when enforcing limits for the given query logic. If no match was found, an empty
         * {@link Optional} will be returned
         * @param queryLogic the query logic
         * @return the {@link QueryLogicGroupLimit} if found
         */
        private Optional<QueryLogicGroupLimit> getLimit(String queryLogic) {
            // Fetch the best match from the cache, if present.
            Optional<QueryLogicGroupLimit> limit = limitCache.getIfPresent(queryLogic);
            // Otherwise, identify the best match and update the cache.
            if(limit == null) {
                // Look for a query logic group limit originating from a configuration that matches the query logic. The first matching limit will be the best
                // match due to how the limits are sorted, which is:
                // 1. First by matching type: EXACT, then PARTIAL, then ALL
                // 2. Then by query limit, from lowest to highest.
                // 3. Then by group name.
                for (MatchableLimit matchableLimit : configuredLimits) {
                    if(matchableLimit.matcher.matches(queryLogic)) {
                        limit = Optional.of(QueryLogicGroupLimit.fromConfig(matchableLimit.groupName, matchableLimit.queryLimit));
                        break;
                    }
                }
                
                // If no matching limit was found, then cache an empty optional.
                if(limit == null) {
                    limit = Optional.empty();
                }
                
                // Cache it.
                if(log.isTraceEnabled()) {
                    log.trace("Caching query logic group limit for '" + queryLogic + "': " + limit);
                }
                limitCache.put(queryLogic, limit);
            }
            return limit;
        }
        
        /**
         * Return the best matching overridden limit to use when enforcing limits for the given query logic. If no match was for the query logic against any of
         * the groups present in the map of overridden limits, an empty optional will be returned.
         * @param sourceId either a user dn or a system name
         * @param queryLogic the query logic
         * @param overriddenLimits a map of query logic group names to their corresponding overriding limit
         * @return the {@link QueryLogicGroupLimit} if found
         */
        public Optional<QueryLogicGroupLimit> getOverriddenLimit(String sourceId, String queryLogic, Map<String,Integer> overriddenLimits) {
            // The cache key consists of the source id and the query logic.
            String key = sourceId + queryLogic;
            Optional<QueryLogicGroupLimit> optional = overiddenLimitCache.getIfPresent(key);
            // If we do not already have the best match cached, attempt to find it.
            if (optional == null) {
                // Construct a new sorted set of matchable limits that will be sorted in the following manner:
                // 1. First by matching type: EXACT, then PARTIAL, then ALL
                // 2. Then by the overridden query limit, from lowest to highest.
                // 3. Then by the group name.
                // This set will only consist of groups present in the map of overridden limits.
                SortedSet<MatchableLimit> limits = new TreeSet<>();
                for (Map.Entry<String,Integer> entry : overriddenLimits.entrySet()) {
                    String groupName = entry.getKey();
                    Matcher matcher = groupMatchers.get(groupName);
                    if (matcher.matches(queryLogic)) {
                        limits.add(new MatchableLimit(groupName, matcher, entry.getValue()));
                    }
                }
                
                // It's possible for the set to be empty if the query logic did not match against any of the query logic groups that were overridden. In this
                // case, return an empty optional.
                if (limits.isEmpty()) {
                    optional = Optional.empty();
                } else {
                    // Otherwise the best match is the first one.
                    MatchableLimit bestLimit = limits.first();
                    QueryLogicGroupLimit limit = QueryLogicGroupLimit.fromConfig(bestLimit.groupName, bestLimit.queryLimit);
                    optional = Optional.of(limit);
                }
                
                // Cache it.
                if(log.isTraceEnabled()) {
                    log.trace("Caching overridden query logic group limit for '" + key + "': " + optional);
                }
                overiddenLimitCache.put(key, optional);
            }
            return optional;
        }
        
        /**
         * This class represents a sortable query logic group and its limit.
         */
        private static class MatchableLimit implements Comparable<MatchableLimit> {
            
            private final String groupName;
            private final Matcher matcher;
            private final int queryLimit;
            
            public MatchableLimit(String groupName, Matcher matcher, int queryLimit) {
                this.groupName = groupName;
                this.matcher = matcher;
                this.queryLimit = queryLimit;
            }
            
            @Override
            public int compareTo(MatchableLimit o) {
                // First sort by the matcher type, sorting in order EXACT, PARTIAL, then ALL.
                int comparison = matcher.getType().compareTo(o.matcher.getType());
                
                // Then sort by the query limit from lowest to highest.
                if (comparison == 0) {
                    comparison = Integer.compare(queryLimit, o.queryLimit);
                }
                
                // Then sort by the group name. In practice, this should always be unique.
                if (comparison == 0) {
                    comparison = groupName.compareTo(o.groupName);
                }
                return comparison;
            }
        }
    }
    
    /**
     * This class is responsible for extracting the limits from {@link UserConfiguration} instances and then identifying the query limits to enforce for
     * individual users, overridden or otherwise.
     */
    private class UserLimitProvider {
        
        // Cache of user dns to their query limits.
        private final Cache<String, UserQueryLimit> cache = Caffeine.newBuilder().build();
        
        // Map of user dns to configured query limits.
        private Map<String, UserQueryLimit> configuredLimits;
        
        private UserLimitProvider(Collection<UserConfiguration> configs, Set<String> queryLogicGroups) {
            if(configs != null && !configs.isEmpty()) {
                validateConfigs(configs, queryLogicGroups);
                populateLimits(configs);
            } else {
                this.configuredLimits = Map.of();
            }
        }
        
        /**
         * Validate the given configurations.
         * @param configs the configurations to validate
         */
        private void validateConfigs(Collection<UserConfiguration> configs, Set<String> queryLogicGroups) {
            Set<String> userDns = new HashSet<>();
            for(UserConfiguration config : configs) {
                // Verify that a user dn was given.
                String userDn = config.getUserDn();
                if(StringUtils.isBlank(userDn)) {
                    throw new IllegalArgumentException("User query limit configuration given with blank user DN");
                }
                
                // Verify we have not seen a configuration with the user dn before.
                if (userDns.contains(userDn)) {
                    throw new IllegalArgumentException("Multiple query limit configurations specified for user '" + userDn + "'");
                } else {
                    userDns.add(userDn);
                }
                
                // Verify that if the user query limit was overridden, it is not negative.
                if(config.getQueryLimit() != null && config.getQueryLimit() < 0) {
                    throw new IllegalArgumentException("Negative user query limit given for user '" + userDn + "'");
                }
                
                // Verify that no non-existent query logic groups were specified.
                Map<String,Integer> groupLimits = config.getQueryLogicGroupLimits();
                if(groupLimits != null && !groupLimits.isEmpty()) {
                    Set<String> groups = groupLimits.keySet();
                    Sets.SetView<String> nonExistentGroups = Sets.difference(groups, queryLogicGroups);
                    if(!nonExistentGroups.isEmpty()) {
                        throw new IllegalArgumentException("Non-existent query logic groups specified for query limit configuration for user '" + userDn + "': " + nonExistentGroups);
                    }
                }
            }
        }
        
        /**
         * Populate the limits to enforce for users.
         * @param configs the configs to populate the limits from
         */
        private void populateLimits(Collection<UserConfiguration> configs) {
            Map<String, UserQueryLimit> configuredLimits = new HashMap<>();
            for(UserConfiguration config : configs) {
                // If the query limit given for the user was null or less than zero, use the default user query limit.
                Integer queryLimit = config.getQueryLimit();
                if(queryLimit == null || queryLimit < 0) {
                    queryLimit = QueryLimitProvider.this.config.getDefaultUserQueryLimit();
                }
                configuredLimits.put(config.getUserDn(), UserQueryLimit.fromConfig(config.getUserDn(), queryLimit, config.getQueryLogicGroupLimits()));
            }
            this.configuredLimits = Map.copyOf(configuredLimits);
        }
        
        /**
         * Return the best matching {@link UserQueryLimit} to use when enforcing limits for the given user. If no match was found to a configured limit, then
         * default limits will be returned.
         * @param userDn the user DN
         * @return the {@link QueryLogicGroupLimit} if found
         */
        private UserQueryLimit getLimit(String userDn) {
            // Fetch the limits from the cache if present.
            UserQueryLimit limit = cache.getIfPresent(userDn);
            if(limit == null) {
                // Otherwise attempt to get it from the map of configured limits.
                limit = configuredLimits.get(userDn);
                // If no match was found, use the default user query limit.
                if(limit == null) {
                    limit = UserQueryLimit.fromDefaults(userDn, config.getDefaultUserQueryLimit());
                }
                
                // Put it in the cache.
                if(log.isTraceEnabled()) {
                    log.trace("Caching user query limit for '" + userDn + "': " + limit);
                }
                cache.put(userDn, limit);
            }
            return limit;
        }
    }
    
    /**
     * This class is responsible for extracting the limits from {@link SystemConfiguration} instances and then identifying the query limits to enforce for
     * individual systems, overridden or otherwise.
     */
    private class SystemLimitProvider {
        
        // Cache of systems to their best limit.
        private final Cache<String, SystemQueryLimit> limitCache = Caffeine.newBuilder().build();
        // Cache of systems to whether queries submitted on them should count against user query limits.
        private final Cache<String, Boolean> countsAgainstUserLimitCache = Caffeine.newBuilder().build();
        // The set of query limits in sorted order of best match to worst.
        private SortedSet<MatchableLimit> configuredLimits;
        
        private SystemLimitProvider(Collection<SystemConfiguration> configs, Set<String> queryLogicGroups) {
            if(configs != null && !configs.isEmpty()) {
                validateConfigs(configs, queryLogicGroups);
                populateLimits(configs);
            } else {
                this.configuredLimits = Collections.emptySortedSet();
            }
        }
        
        /**
         * Validate the given configurations.
         * @param configs the configurations to validate
         */
        private void validateConfigs(Collection<SystemConfiguration> configs, Set<String> queryLogicGroups) {
            Set<String> systemPatterns = new HashSet<>();
            Map<String, String> matcherPatterns = new HashMap<>();
            for(SystemConfiguration config : configs) {
                // Verify that a system pattern was given.
                String systemPattern = config.getSystemPattern();
                if(StringUtils.isBlank(systemPattern)) {
                    throw new IllegalArgumentException("System query limit configuration specified with blank system pattern");
                }
                
                // Verify that the pattern compiles if it is not simply a * as is occasionally used as a wildcard in configurations.
                try {
                    if (!systemPattern.equals(ASTERISK)) {
                        Pattern.compile(systemPattern);
                    }
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regex in system pattern '" + systemPattern + "'", e);
                }
                
                // Verify that we have not seen a configuration with the system pattern before.
                if(systemPatterns.contains(systemPattern)) {
                    throw new IllegalArgumentException("Multiple query limit configurations specified with system pattern '" + systemPattern + "'");
                } else {
                    systemPatterns.add(systemPattern);
                }
                
                // Fetch the matcher that would be used for the system pattern.
                Matcher matcher;
                try {
                    matcher = getMatcher(systemPattern);
                } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                    throw new IllegalArgumentException("Failed to analyze regex for system pattern '" + systemPattern + "': " + e.getMessage(), e);
                }
                
                // Verify that we do not have an exact-matching pattern that is equivalent to a previously seen exact-matching pattern, such as 'SYSTEM-01' vs.
                // 'SYSTEM\\-01'.
                if(matcher instanceof StringMatcher) {
                    String matcherPattern = ((StringMatcher) matcher).getValue();
                    String equivalentSystemPattern = matcherPatterns.get(matcherPattern);
                    if(equivalentSystemPattern != null) {
                        throw new IllegalArgumentException("System pattern '" + systemPattern +
                                        "' will resolve to an exact match that is equivalent to system pattern '" + equivalentSystemPattern +
                                        "' from another system configuration.");
                    } else {
                        matcherPatterns.put(matcherPattern, systemPattern);
                    }
                }
                
                // Verify that we do not have a negative query limit.
                if(config.getQueryLimit() != null && config.getQueryLimit() < 0) {
                    throw new IllegalArgumentException("Negative query limit specified for system pattern '" + systemPattern + "'");
                }
                
                // Safeguard against allowing a configuration to potentially set whether queries on a system counts against user limits to false for all
                // systems. Only allow this to be done for exact system names, or non-wildcard-only patterns.
                if (wildcardOnlyPattern.matcher(systemPattern).matches() && !config.getCountsAgainstsUserLimit()) {
                    throw new IllegalArgumentException("System pattern '" + systemPattern +
                                    "' is wildcard-only and may not be used to override whether queries count against user limits to false");
                }
                
                // Verify that no non-existent query logic groups were specified.
                Map<String,Integer> groupLimits = config.getQueryLogicGroupLimits();
                if(groupLimits != null && !groupLimits.isEmpty()) {
                    Set<String> groups = groupLimits.keySet();
                    Sets.SetView<String> nonExistentGroups = Sets.difference(groups, queryLogicGroups);
                    if(!nonExistentGroups.isEmpty()) {
                        throw new IllegalArgumentException("Non-existent query logic groups given for system pattern '" + systemPattern + "': " + nonExistentGroups);
                    }
                }
            }
        }
        
        /**
         * Populate the limits to enforce for systems.
         * @param configs the configs to populate the limits from
         */
        private void populateLimits(Collection<SystemConfiguration> configs) {
            SortedSet<MatchableLimit> configuredLimits = new TreeSet<>();
            for(SystemConfiguration systemConfig : configs) {
                
                // Identify the best matching strategy to use for matching system names.
                String systemPattern = systemConfig.getSystemPattern();
                Matcher matcher;
                try {
                    matcher = getMatcher(systemPattern);
                } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                    throw new IllegalArgumentException("Failed to analyze regex for system pattern '" + systemPattern + "': " + e.getMessage(), e);
                }
                
                // If the query limit given for the system was null or less than zero, use the default system query limit.
                Integer queryLimit = systemConfig.getQueryLimit();
                if(queryLimit == null || queryLimit < 0) {
                    queryLimit = config.getDefaultSystemQueryLimit();
                }
                
                // If countsAgainstUserLimit is null, use the default.
                Boolean countsAgainstUserLimit = systemConfig.getCountsAgainstsUserLimit();
                if(countsAgainstUserLimit == null) {
                    countsAgainstUserLimit = true;
                }
                
                configuredLimits.add(new MatchableLimit(matcher, systemPattern, queryLimit, countsAgainstUserLimit, systemConfig.getQueryLogicGroupLimits()));
            }
            this.configuredLimits = Collections.unmodifiableSortedSet(configuredLimits);
        }
        
        /**
         * Return the best matching {@link SystemQueryLimit} to use for the given system. If no match was found to a configured limit, then default limits will
         * be returned.
         * @param system the system name
         * @return the {@link SystemQueryLimit}
         */
        private SystemQueryLimit getLimit(String system) {
            // Check if we have the best match already cached.
            SystemQueryLimit limit = limitCache.getIfPresent(system);
            if(limit == null) {
                // If not, attempt to find one from configured limits. The first matching limit will be the best one due to how the limits are sorted, which is:
                // 1. First by matching type: EXACT, then PARTIAL, then ALL
                // 2. Then by query limit, from lowest to highest.
                // 3. Then by whether queries should apply to the user query limit, from true to false.
                // 4. Then by system pattern.
                for (MatchableLimit matchableLimit : configuredLimits) {
                    if(matchableLimit.matcher.matches(system)) {
                        limit = SystemQueryLimit.fromConfig(matchableLimit.systemPattern, matchableLimit.queryLimit, matchableLimit.countsAgainstUserLimit, matchableLimit.queryLogicGroupLimits);
                        break;
                    }
                }
                
                // If a matching configured limit was not found, construct one from the default limits.
                if(limit == null) {
                    limit = SystemQueryLimit.fromDefaults(system, config.getDefaultSystemQueryLimit());
                }
                
                // Cache it.
                if(log.isTraceEnabled()) {
                    log.trace("Caching system query limit for '" + system + "': " + limit);
                }
                limitCache.put(system, limit);
            }
            return limit;
        }
        
        /**
         * Return whether queries count against the user query limit when submitted on the given system.
         * @param system the system name
         * @return true if queries count against the user query limit, or false otherwise
         */
        private boolean countsAgainstUserLimit(String system) {
            // Check if we already have an evaluation for this cached.
            Boolean countsAgainstUserLimit = countsAgainstUserLimitCache.getIfPresent(system);
            if(countsAgainstUserLimit == null) {
                // If not, attempt to determine whether query limits apply based on the configured limits.
                for(MatchableLimit matchableLimit : configuredLimits) {
                    // Wildcard system patterns are not allowed to override countsAgainstUserLimit to false. If we encounter match type ALL, all remaining
                    // limits have wildcard system patterns and can be skipped.
                    if (matchableLimit.matcher.getType() == Matcher.Type.ALL) {
                        break;
                    }
                    
                    // If we found a match, update the counts against user limit. If countsAgainstUserLimit is true or this is an exact match, we do not need
                    // to evaluate any other matches.
                    if(matchableLimit.matcher.matches(system)) {
                        countsAgainstUserLimit = matchableLimit.countsAgainstUserLimit;
                        if(countsAgainstUserLimit || matchableLimit.matcher.getType() == Matcher.Type.EXACT) {
                            break;
                        }
                    }
                }
                
                // If we did not find any configured limit for the system, use the default value.
                if(countsAgainstUserLimit == null) {
                    countsAgainstUserLimit = true;
                }
                
                // Cache it.
                if(log.isTraceEnabled()) {
                    log.trace("Caching countsAgainstUserLimitCache for '" + system + "': " + countsAgainstUserLimitCache);
                }
                countsAgainstUserLimitCache.put(system, countsAgainstUserLimit);
            }
            return countsAgainstUserLimit;
        }
        
        /**
         * This class represents a sortable system pattern and its limit configuration.
         */
        private class MatchableLimit implements Comparable<MatchableLimit> {
            private final Matcher matcher;
            private final String systemPattern;
            private final int queryLimit;
            private final boolean countsAgainstUserLimit;
            private final Map<String, Integer> queryLogicGroupLimits;
            
            public MatchableLimit(Matcher matcher, String systemPattern, int queryLimit, boolean countsAgainstUserLimit, Map<String,Integer> queryLogicGroupLimits) {
                this.matcher = matcher;
                this.systemPattern = systemPattern;
                this.queryLimit = queryLimit;
                this.countsAgainstUserLimit = countsAgainstUserLimit;
                this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : Map.copyOf(queryLogicGroupLimits);
            }
            
            @Override
            public int compareTo(MatchableLimit o) {
                // First sort by the type, sorting in order EXACT, PARTIAL, then ALL.
                int comparison = matcher.getType().compareTo(o.matcher.getType());
                
                // Then sort by the query limit from lowest to highest.
                if(comparison == 0) {
                    comparison = Integer.compare(queryLimit, o.queryLimit);
                }
                
                // Then sort by whether queries count against the user limit, true to false.
                if (comparison == 0) {
                    comparison = Boolean.compare(o.countsAgainstUserLimit, countsAgainstUserLimit);
                }
                
                // Then sort by the system pattern. In practice, this should always be unique.
                if(comparison == 0) {
                    comparison = systemPattern.compareTo(o.systemPattern);
                }
                
                return comparison;
            }
        }
    }
}
