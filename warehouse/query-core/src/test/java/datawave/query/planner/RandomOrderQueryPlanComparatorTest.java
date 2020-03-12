package datawave.query.planner;

import com.google.common.collect.Lists;
import datawave.common.test.utils.query.RangeFactoryForTests;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.comparator.RandomOrderQueryPlanComparator;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RandomOrderQueryPlanComparatorTest {
    
    @Test
    public void testSameComparatorSameOrder() throws ParseException {
        
        RandomOrderQueryPlanComparator comparator = new RandomOrderQueryPlanComparator();
        
        List<QueryPlan> plansA = buildQueryPlans();
        List<QueryPlan> plansB = new ArrayList<>(plansA);
        
        PriorityBlockingQueue<QueryPlan> queueA = new PriorityBlockingQueue<>(plansA.size(), comparator);
        queueA.addAll(plansA);
        
        PriorityBlockingQueue<QueryPlan> queueB = new PriorityBlockingQueue<>(plansB.size(), comparator);
        queueB.addAll(plansB);
        
        // Confirm that the same comparator sorts the same list in the same order.
        assertTrue(equalsQueues(queueA, queueB));
    }
    
    @Test
    public void testDifferentComparatorDifferentOrder() throws ParseException {
        RandomOrderQueryPlanComparator comparatorA = new RandomOrderQueryPlanComparator();
        RandomOrderQueryPlanComparator comparatorB = new RandomOrderQueryPlanComparator();
        
        List<QueryPlan> plansA = buildQueryPlans();
        List<QueryPlan> plansB = new ArrayList<>(plansA);
        
        PriorityBlockingQueue<QueryPlan> queueA = new PriorityBlockingQueue<>(plansA.size(), comparatorA);
        queueA.addAll(plansA);
        
        PriorityBlockingQueue<QueryPlan> queueB = new PriorityBlockingQueue<>(plansB.size(), comparatorB);
        queueB.addAll(plansB);
        
        // Confirm that the same comparator sorts the same list in the same order.
        assertFalse(equalsQueues(queueA, queueB));
    }
    
    private List<QueryPlan> buildQueryPlans() throws ParseException {
        List<QueryPlan> plans = new ArrayList<>();
        String terms = "0123456789";
        for (char c : terms.toCharArray()) {
            String queryString = "FOO == '" + c + "'";
            JexlNode node = JexlASTHelper.parseJexlQuery(queryString);
            Range range = RangeFactoryForTests.makeShardedRange("20190314_" + c);
            Iterable<Range> rangeIterable = Lists.newArrayList(range);
            plans.add(new QueryPlan(queryString, node, rangeIterable));
        }
        return plans;
    }
    
    private boolean equalsQueues(PriorityBlockingQueue<QueryPlan> a, PriorityBlockingQueue<QueryPlan> b) {
        while (a.size() > 0 && b.size() > 0) {
            if (!a.poll().equals(b.poll()))
                return false;
        }
        return true;
    }
}
