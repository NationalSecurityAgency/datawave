package datawave.query.rules;

import java.util.Objects;
import java.util.StringJoiner;

public abstract class AbstractQueryRule implements QueryRule {

    protected String name;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public AbstractQueryRule() {}

    public AbstractQueryRule(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        AbstractQueryRule that = (AbstractQueryRule) object;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", getClass().getSimpleName() + "[", "]").add("name='" + name + "'").toString();
    }
}
