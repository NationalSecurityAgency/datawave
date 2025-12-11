package datawave.query.planner;

import java.util.HashSet;
import java.util.Set;

public class Plans {
    private String initialPlan;
    private Set<String> plans = new HashSet<>();

    public String getInitialPlan() {
        return initialPlan;
    }

    public void setInitialPlan(String initialPlan) {
        this.initialPlan = initialPlan;
    }

    public Set<String> getPlans() {
        return plans;
    }

    public void addPlan(String plan) {
        this.plans.add(plan);
    }
}
