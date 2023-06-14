package datawave.iterators.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * A constructed trie which can scan a string for tokens and return a ttl value. A TtlTrie is an immutable object, and as such is inherently threadsafe.
 */
public final class TokenTtlTrie {
    private static final Logger log = Logger.getLogger(TokenTtlTrie.class);
    protected static final short DELIMITER_CHAR_CLASS = -1;
    protected static final short UNRECOGNIZED_CHAR_CLASS = -2;
    protected static final int REJECT_TOKEN = -1;

    protected final int charClassCount;
    protected final short[] charClasses;
    protected final int[] transitionTable;
    protected final int[] statePriorities;
    protected final long[] stateTTLs;

    TokenTtlTrie(int charClassCount, short[] charClasses, int[] transitionTable, int[] statePriorities, long[] stateTTLs) {
        this.charClassCount = charClassCount;
        this.charClasses = charClasses;
        this.transitionTable = transitionTable;
        this.statePriorities = statePriorities;
        this.stateTTLs = stateTTLs;
    }

    public int size() {
        return stateTTLs.length;
    }

    /**
     * Scan the specified string for tokens, returning the ttl of the best priority token found, or null if no tokens were found.
     */
    public Long scan(byte[] rawString) {
        int bestPriority = Integer.MAX_VALUE;
        double ttl = 0;
        int curState = 0;
        for (byte b : rawString) {
            short charClass = charClasses[0xff & (int) b];
            if (curState == REJECT_TOKEN) {
                if (charClass == DELIMITER_CHAR_CLASS) {
                    curState = 0;
                }
                continue;
            }
            switch (charClass) {
                case UNRECOGNIZED_CHAR_CLASS:
                    curState = REJECT_TOKEN;
                    continue;
                case DELIMITER_CHAR_CLASS:
                    int statePriority = statePriorities[curState];
                    if (statePriority < bestPriority) {
                        bestPriority = statePriority;
                        ttl = stateTTLs[curState];
                    }
                    curState = 0;
                    continue;
                default:
                    curState = transitionTable[curState * charClassCount + charClass];
                    break;
            }
        }
        // Check the last token's state, as it might not have a delimiter
        if (curState != REJECT_TOKEN) {
            int statePriority = statePriorities[curState];
            if (statePriority < bestPriority) {
                bestPriority = statePriority;
                ttl = stateTTLs[curState];
            }
        }
        return bestPriority == Integer.MAX_VALUE ? null : (long) ttl;
    }

    /**
     * Trie construction.
     */
    public static class Builder extends TokenSpecParser<Builder> {
        private int entryCount = 0;
        private final List<Map<Byte,Integer>> transitionMaps = new ArrayList<>();
        private final List<Long> stateTtlList = new ArrayList<>();
        private final List<Integer> statePriorityList = new ArrayList<>();
        private final Set<Byte> delimiters = new HashSet<>();
        private boolean isMerge;

        public enum MERGE_MODE {
            ON, OFF
        };

        Builder() {
            this(MERGE_MODE.OFF);
        }

        Builder(MERGE_MODE mergeMode) {
            transitionMaps.add(new HashMap<>());
            stateTtlList.add(null);
            statePriorityList.add(null);
            this.isMerge = mergeMode.equals(MERGE_MODE.ON);
        }

        public int size() {
            return entryCount;
        }

        /**
         * Set the delimiter set.
         */
        public Builder setDelimiters(byte[] delimiters) {
            this.delimiters.clear();
            for (byte b : delimiters) {
                this.delimiters.add(b);
            }
            return this;
        }

        /**
         * Add a token to the TtlTrie under construction, along with the TTL value the specified token should be associated with.
         */
        @Override
        public Builder addToken(byte[] token, long ttl) {
            int curState = 0;
            for (byte b : token) {
                Map<Byte,Integer> transMap = transitionMaps.get(curState);
                if (transMap.containsKey(b)) {
                    curState = transMap.get(b);
                } else {
                    curState = transitionMaps.size();
                    transitionMaps.add(new HashMap<>());
                    stateTtlList.add(null);
                    statePriorityList.add(null);
                    transMap.put(b, curState);
                }
            }
            int myPriority = ++entryCount;
            // if this is a merge of two configs, override ttl with latest seen
            if (stateTtlList.get(curState) == null) {
                stateTtlList.set(curState, ttl);
                statePriorityList.set(curState, myPriority);
            } else if (isMerge) {
                // maintain original priority just update the ttl
                stateTtlList.set(curState, ttl);
            } else {
                throw new IllegalArgumentException(
                                String.format("Token '%s'(#%d) already specified at index %d", new String(token), statePriorityList.get(curState), myPriority));
            }
            return this;
        }

        public TokenTtlTrie build() {
            long startTime = System.currentTimeMillis();

            // To compress the transition table width, only create transition table entries for characters which are
            // actually relevant during parsing.
            short[] charClasses = new short[256];
            for (int i = 0; i < 256; i++) {
                charClasses[i] = TokenTtlTrie.UNRECOGNIZED_CHAR_CLASS;
            }
            Set<Byte> seenBytes = new HashSet<>();
            for (Map<Byte,Integer> transMap : transitionMaps) {
                seenBytes.addAll(transMap.keySet());
            }
            List<Byte> classReps = new ArrayList<>(seenBytes);
            for (int i = 0; i < classReps.size(); i++) {
                byte b = classReps.get(i);
                charClasses[0xff & (int) b] = (short) i;
            }
            int charClassCount = classReps.size();

            for (byte b : delimiters) {
                if (charClasses[0xff & (int) b] != TokenTtlTrie.UNRECOGNIZED_CHAR_CLASS) {
                    throw new IllegalArgumentException(String.format("Some token contains delimiter %c", (char) b));
                }
                charClasses[0xff & (int) b] = TokenTtlTrie.DELIMITER_CHAR_CLASS;
            }

            // Next build the raw transition table. For each state and each character class, the transition table
            // holds the index of the next state.
            int[] transitionTable = new int[transitionMaps.size() * charClassCount];
            int[] statePriorities = new int[transitionMaps.size()];
            long[] stateTtls = new long[transitionMaps.size()];

            for (int state = 0; state < transitionMaps.size(); state++) {
                Map<Byte,Integer> transMap = transitionMaps.get(state);
                stateTtls[state] = stateTtlList.get(state) == null ? Long.MAX_VALUE : stateTtlList.get(state);
                statePriorities[state] = statePriorityList.get(state) == null ? Integer.MAX_VALUE : statePriorityList.get(state);

                for (int charClass = 0; charClass < classReps.size(); charClass++) {
                    Integer nextState = transMap.get(classReps.get(charClass));
                    if (nextState == null) {
                        transitionTable[state * charClassCount + charClass] = TokenTtlTrie.REJECT_TOKEN;
                    } else {
                        transitionTable[state * charClassCount + charClass] = nextState;
                    }
                }
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Constructed trie on %d entries with %d states and %d character classes in %dms", this.entryCount,
                                transitionMaps.size(), charClassCount, System.currentTimeMillis() - startTime));
            }

            return new TokenTtlTrie(charClassCount, charClasses, transitionTable, statePriorities, stateTtls);
        }
    }
}
