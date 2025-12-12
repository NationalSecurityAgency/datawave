package datawave.query.planner;

import java.util.HashSet;
import java.util.Set;

public class Plans {
    private Set<String> plans = new HashSet<>();

    public Set<String> getPlans() {
        return plans;
    }

    public void addPlan(String plan) {
        this.plans.add(plan);
    }
}
