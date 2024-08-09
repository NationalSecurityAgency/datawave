package datawave.core.query.language.tree;

public class Optimization {
    public enum Type {
        NONE, AND, NOT
    }

    private Type optimizationType = Type.NONE;

    Optimization(Type optimizationType) {
        this.optimizationType = optimizationType;
    }

    Optimization(Optimization other) {
        this.optimizationType = other.optimizationType;
    }

    public Type getType() {
        return optimizationType;
    }
}
