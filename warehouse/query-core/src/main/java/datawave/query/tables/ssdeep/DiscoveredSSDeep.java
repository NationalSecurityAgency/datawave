package datawave.query.tables.ssdeep;

import datawave.query.discovery.DiscoveredThing;

/**
 * Captures a ssdeep query, matching ssdeep and the discovery data about that match. This class immutable once created
 */
public class DiscoveredSSDeep {
    /** A scored match between two ssdeep hashes, output by the SSDeep similarity query logic */
    public final ScoredSSDeepPair scoredSSDeepPair;
    /** The discovered information about the matching SSDeep hash */
    public final DiscoveredThing discoveredThing;

    public DiscoveredSSDeep(ScoredSSDeepPair scoredSSDeepPair, DiscoveredThing discoveredThing) {
        this.scoredSSDeepPair = scoredSSDeepPair;
        this.discoveredThing = discoveredThing;
    }

    public ScoredSSDeepPair getScoredSSDeepPair() {
        return scoredSSDeepPair;
    }

    public DiscoveredThing getDiscoveredThing() {
        return discoveredThing;
    }
}
