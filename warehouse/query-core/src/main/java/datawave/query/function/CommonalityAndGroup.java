package datawave.query.function;

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Represents a commonality key and grouping token.
 */
public class CommonalityAndGroup {
    private final String keyCommonality;
    private final String group;

    public CommonalityAndGroup(String keyCommonality, @Nullable String group) {
        this.keyCommonality = keyCommonality;
        this.group = group;
    }

    /**
     * The key commonality in the token
     *
     * @return the key commonality
     */
    public String getKeyCommonality() {
        return keyCommonality;
    }

    /**
     * The grouping data in the token.
     *
     * @return the grouping
     */
    public String getGroup() {
        return group;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CommonalityAndGroup))
            return false;

        CommonalityAndGroup that = (CommonalityAndGroup) o;
        return keyCommonality.equals(that.keyCommonality) && Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        int result = keyCommonality.hashCode();
        result = 31 * result + Objects.hashCode(group);
        return result;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
