package datawave.query.analysis;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeMap;

import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryClusterer.Group;

/**
 * Incremental, deterministic workload selection with one query per active group per round. Diversity is approximate: candidates and retained landmarks are
 * bounded. Every distinct text/syntax pair is eventually emitted once; occurrences and original indexes are retained in each selection.
 */
public final class QueryWorkloadSelector {
    public Cursor cursor(QueryClusterer.Result groups) {
        return cursor(groups, new Options());
    }

    public Cursor cursor(QueryClusterer.Result groups, Options options) {
        return new Cursor(Objects.requireNonNull(groups, "groups"), Objects.requireNonNull(options, "options"));
    }

    public static final class Options {
        private final int groupWindow;
        private final int memberWindow;
        private final int firstLandmarks;
        private final int recentLandmarks;

        public Options() {
            this(16, 8, 4, 4);
        }

        public Options(int groupWindow, int memberWindow, int firstLandmarks, int recentLandmarks) {
            if (groupWindow < 1 || memberWindow < 1 || firstLandmarks < 1 || recentLandmarks < 1) {
                throw new IllegalArgumentException("Candidate windows and landmark limits must be positive");
            }
            this.groupWindow = groupWindow;
            this.memberWindow = memberWindow;
            this.firstLandmarks = firstLandmarks;
            this.recentLandmarks = recentLandmarks;
        }

        public int getGroupWindow() {
            return groupWindow;
        }

        public int getMemberWindow() {
            return memberWindow;
        }

        public int getFirstLandmarks() {
            return firstLandmarks;
        }

        public int getRecentLandmarks() {
            return recentLandmarks;
        }
    }

    /** Mutable, non-thread-safe in-memory cursor. A fresh cursor on the same result reproduces the same sequence. */
    public static final class Cursor implements Iterator<Selection> {
        private final QueryClusterer.Result result;
        private final Options options;
        private final List<GroupState> active = new ArrayList<>();
        private final Deque<GroupState> waiting = new ArrayDeque<>();
        private final List<GroupState> window = new ArrayList<>();
        private final List<QueryFingerprint> first = new ArrayList<>();
        private final Deque<QueryFingerprint> recent = new ArrayDeque<>();
        private final Set<String> coveredProfiles = new HashSet<>();
        private int remaining;
        private int emitted;
        private int round;
        private int coveredGroups;
        private int coveredFingerprints;
        private int representedOccurrences;
        private double minimumDiversityDistance = 1;
        private double totalDiversityDistance;
        private long comparisons;

        private Cursor(QueryClusterer.Result result, Options options) {
            this.result = result;
            this.options = options;
            for (Group group : result.getGroups()) {
                GroupState state = new GroupState(group);
                active.add(state);
                remaining += state.remaining;
            }
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public Selection next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Query workload exhausted");
            }
            if (window.isEmpty() && waiting.isEmpty()) {
                beginRound();
            }
            replenish();
            GroupState bestGroup = null;
            DistinctQuery bestQuery = null;
            double bestDistance = -1;
            for (GroupState group : window) {
                for (DistinctQuery candidate : group.candidates(options.memberWindow)) {
                    double distance = distance(candidate.query.getFingerprint(), group);
                    if (distance > bestDistance || (distance == bestDistance && before(group, candidate, bestGroup, bestQuery))) {
                        bestGroup = group;
                        bestQuery = candidate;
                        bestDistance = distance;
                    }
                }
            }
            QueryFingerprint fingerprint = bestQuery.query.getFingerprint();
            if (!bestGroup.started) {
                coveredGroups++;
            }
            if (bestGroup.unseen.containsKey(fingerprint.getKey())) {
                coveredFingerprints++;
            }
            coveredProfiles.add(fingerprint.getProtectedProfile());
            representedOccurrences += bestQuery.indexes.size();
            // The first selection has no landmarks, so its default distance is not a diversity observation.
            if (emitted > 0) {
                minimumDiversityDistance = Math.min(minimumDiversityDistance, bestDistance);
                totalDiversityDistance += bestDistance;
            }
            bestGroup.consume(bestQuery);
            window.remove(bestGroup);
            remaining--;
            emitted++;
            if (first.size() < options.firstLandmarks) {
                first.add(fingerprint);
            }
            recent.addLast(fingerprint);
            if (recent.size() > options.recentLandmarks) {
                recent.removeFirst();
            }
            return new Selection(bestQuery, bestGroup.group.getId(), round, bestDistance);
        }

        public List<Selection> take(int limit) {
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be nonnegative");
            }
            List<Selection> selections = new ArrayList<>(Math.min(limit, remaining));
            while (selections.size() < limit && hasNext()) {
                selections.add(next());
            }
            return Collections.unmodifiableList(selections);
        }

        public int getRemainingCount() {
            return remaining;
        }

        public int getEmittedCount() {
            return emitted;
        }

        public long getComparisonCount() {
            return comparisons;
        }

        public Options getOptions() {
            return options;
        }

        QueryClusterer.Result getClusteringResult() {
            return result;
        }

        int getCurrentRound() {
            return round;
        }

        int getCoveredGroupCount() {
            return coveredGroups;
        }

        int getCoveredFingerprintCount() {
            return coveredFingerprints;
        }

        int getCoveredProfileCount() {
            return coveredProfiles.size();
        }

        int getRepresentedOccurrenceCount() {
            return representedOccurrences;
        }

        OptionalDouble getMinimumDiversityDistance() {
            return emitted < 2 ? OptionalDouble.empty() : OptionalDouble.of(minimumDiversityDistance);
        }

        OptionalDouble getMeanDiversityDistance() {
            return emitted < 2 ? OptionalDouble.empty() : OptionalDouble.of(totalDiversityDistance / (emitted - 1));
        }

        private boolean before(GroupState group, DistinctQuery query, GroupState bestGroup, DistinctQuery bestQuery) {
            if (bestGroup == null) {
                return true;
            }
            int order = group.group.getId().compareTo(bestGroup.group.getId());
            return order < 0 || (order == 0 && query.key.compareTo(bestQuery.key) < 0);
        }

        private double distance(QueryFingerprint candidate, GroupState group) {
            double minimum = 1;
            for (QueryFingerprint landmark : first) {
                comparisons++;
                minimum = Math.min(minimum, QuerySimilarity.distance(candidate, landmark));
            }
            for (QueryFingerprint landmark : recent) {
                comparisons++;
                minimum = Math.min(minimum, QuerySimilarity.distance(candidate, landmark));
            }
            if (group.started) {
                comparisons++;
                minimum = Math.min(minimum, QuerySimilarity.distance(candidate, group.group.getRepresentative().getFingerprint()));
            }
            return minimum;
        }

        private void beginRound() {
            round++;
            active.removeIf(group -> group.remaining == 0);
            Map<String,Deque<GroupState>> profiles = new TreeMap<>();
            for (GroupState group : active) {
                profiles.computeIfAbsent(group.group.getRepresentative().getFingerprint().getProtectedProfile(), key -> new ArrayDeque<>()).addLast(group);
            }
            // Remove exhausted profile queues immediately: rebuilding a round visits each live group only once.
            while (!profiles.isEmpty()) {
                Iterator<Deque<GroupState>> queues = profiles.values().iterator();
                while (queues.hasNext()) {
                    Deque<GroupState> queue = queues.next();
                    waiting.addLast(queue.removeFirst());
                    if (queue.isEmpty()) {
                        queues.remove();
                    }
                }
            }
        }

        private void replenish() {
            while (window.size() < options.groupWindow && !waiting.isEmpty()) {
                window.add(waiting.removeFirst());
            }
        }
    }

    private static final class GroupState {
        private final Group group;
        private final Map<String,Deque<DistinctQuery>> unseen = new TreeMap<>();
        private final Map<String,Deque<DistinctQuery>> seen = new TreeMap<>();
        private final DistinctQuery representative;
        private int remaining;
        private boolean started;

        private GroupState(Group group) {
            this.group = group;
            Map<String,DistinctQuery> distinct = new TreeMap<>();
            for (QueryAnalysis query : group.getMembers()) {
                distinct.computeIfAbsent(QueryFingerprint.inputKey(query), key -> new DistinctQuery(query)).indexes.add(query.getIndex());
            }
            for (DistinctQuery query : distinct.values()) {
                Collections.sort(query.indexes);
                unseen.computeIfAbsent(query.query.getFingerprint().getKey(), key -> new ArrayDeque<>()).addLast(query);
            }
            remaining = distinct.size();
            representative = distinct.get(QueryFingerprint.inputKey(group.getRepresentative()));
        }

        private List<DistinctQuery> candidates(int limit) {
            if (!started) {
                return Collections.singletonList(representative);
            }
            List<DistinctQuery> candidates = new ArrayList<>();
            // Exhaust unvisited fingerprints before taking another literal variant from an already visited fingerprint.
            Map<String,Deque<DistinctQuery>> source = unseen.isEmpty() ? seen : unseen;
            for (Deque<DistinctQuery> bucket : source.values()) {
                candidates.add(bucket.getFirst());
                if (candidates.size() == limit) {
                    break;
                }
            }
            return candidates;
        }

        private void consume(DistinctQuery query) {
            String key = query.query.getFingerprint().getKey();
            Deque<DistinctQuery> bucket = unseen.remove(key);
            if (bucket == null) {
                bucket = seen.remove(key);
            }
            // Representatives and candidate heads follow the same stable text ordering.
            if (bucket.removeFirst() != query) {
                throw new IllegalStateException("Selection did not reference the next query in its fingerprint bucket");
            }
            if (!bucket.isEmpty()) {
                seen.put(key, bucket);
            }
            started = true;
            remaining--;
        }
    }

    private static final class DistinctQuery {
        private final QueryAnalysis query;
        private final String key;
        private final List<Integer> indexes = new ArrayList<>();

        private DistinctQuery(QueryAnalysis query) {
            this.query = query;
            key = QueryFingerprint.inputKey(query);
        }
    }

    public static final class Selection {
        private final QueryAnalysis query;
        private final String groupId;
        private final int round;
        private final double diversityDistance;
        private final List<Integer> inputIndexes;

        private Selection(DistinctQuery query, String groupId, int round, double diversityDistance) {
            this.query = query.query;
            this.groupId = groupId;
            this.round = round;
            this.diversityDistance = diversityDistance;
            inputIndexes = Collections.unmodifiableList(new ArrayList<>(query.indexes));
        }

        public QueryAnalysis getQuery() {
            return query;
        }

        public String getGroupId() {
            return groupId;
        }

        public int getRound() {
            return round;
        }

        /** Minimum distance to retained landmarks among the bounded candidates considered at selection time; not a global optimum. */
        public double getDiversityDistance() {
            return diversityDistance;
        }

        public int getOccurrenceCount() {
            return inputIndexes.size();
        }

        public List<Integer> getInputIndexes() {
            return inputIndexes;
        }
    }
}
