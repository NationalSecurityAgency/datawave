package datawave.query.planner.comparator;

import datawave.query.planner.QueryPlan;

import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Return query plans in random order based on the query string, range, and magic.
 *
 * The following config options should be considered. <code>
 * {@link datawave.query.config.ShardQueryConfiguration#numRangesToBuffer}
 * {@link datawave.query.config.ShardQueryConfiguration#rangeBufferPollMillis}
 * {@link datawave.query.config.ShardQueryConfiguration#rangeBufferTimeoutMillis}
 * </code>
 */
public class RandomOrderQueryPlanComparator implements Comparator<QueryPlan> {
    
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    int x = rand.nextInt();
    int y = rand.nextInt();
    boolean b = rand.nextBoolean();
    
    /**
     * Compare based on the query string, range, and random integer
     *
     * @param o1
     *            query plan A
     * @param o2
     *            query plan B
     * @return -1, 0, or 1. Randomly.
     */
    @Override
    public int compare(QueryPlan o1, QueryPlan o2) {
        int left = (o1.getQueryString().hashCode() + o1.getRanges().iterator().next().hashCode()) ^ x;
        int right = (o2.getQueryString().hashCode() + o2.getRanges().iterator().next().hashCode()) ^ y;
        int cmpr = left - right;
        if (b) {
            return cmpr;
        } else {
            return cmpr * -1;
        }
    }
}
