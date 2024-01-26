package datawave.test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import datawave.query.attributes.Attribute;
import datawave.query.common.grouping.AggregateOperation;
import datawave.query.common.grouping.Aggregator;
import datawave.query.common.grouping.FieldAggregator;
import datawave.query.common.grouping.Group;
import datawave.query.common.grouping.GroupingAttribute;

public class GroupAssert extends AbstractAssert<GroupAssert,Group> {

    public static GroupAssert assertThat(Group group) {
        return new GroupAssert(group);
    }

    protected GroupAssert(Group group) {
        super(group, GroupAssert.class);
    }

    public GroupAssert hasCount(int count) {
        isNotNull();
        if (actual.getCount() != count) {
            failWithMessage("Expected count to be %s but was %s", count, actual.getCount());
        }
        return this;
    }

    public GroupAssert hasVisibilitiesForKey(GroupingAttribute<?> key, ColumnVisibility... visibilities) {
        isNotNull();
        Collection<ColumnVisibility> actualVisibilities = actual.getVisibilitiesForAttribute(key);
        Assertions.assertThat(actualVisibilities).isNotNull().withFailMessage("Expected column visibilties for %s to contain exactly %s but was %s", key,
                        Arrays.toString(visibilities), actualVisibilities).containsExactlyInAnyOrder(visibilities);
        return this;
    }

    public GroupAssert hasDocumentVisibilities(ColumnVisibility... visibilities) {
        isNotNull();
        Collection<ColumnVisibility> actualVisibilities = actual.getDocumentVisibilities();
        Assertions.assertThat(actualVisibilities).isNotNull()
                        .withFailMessage("Expected document visibilities to contain exactly %s but was %s", Arrays.toString(visibilities), actualVisibilities)
                        .containsExactlyInAnyOrder(visibilities);
        return this;
    }

    public GroupAssert hasAggregatedSum(String field, BigDecimal sum) {
        return hasAggregation(field, AggregateOperation.SUM, sum);
    }

    public GroupAssert hasAggregatedMax(String field, Object data) {
        assertAggregatedAttributeData(field, AggregateOperation.MAX, data);
        return this;
    }

    public GroupAssert hasAggregatedMin(String field, Object data) {
        assertAggregatedAttributeData(field, AggregateOperation.MIN, data);
        return this;
    }

    private void assertAggregatedAttributeData(String field, AggregateOperation operation, Object data) {
        Object aggregation = getAggregation(field, operation);
        if (aggregation != null) {
            Object actualData = ((Attribute<?>) aggregation).getData();
            if (!Objects.equals(data, actualData)) {
                failWithMessage("Expected %s for field %s to be %s but was %s", operation, field, data, actualData);
            }
        } else {
            failWithMessage("Expected %s for %s to not be null", operation, field);
        }
    }

    public GroupAssert hasAggregatedCount(String field, long count) {
        return hasAggregation(field, AggregateOperation.COUNT, count);
    }

    public GroupAssert hasAggregatedAverage(String field, BigDecimal average) {
        return hasAggregation(field, AggregateOperation.AVERAGE, average);
    }

    public GroupAssert hasAggregation(String field, AggregateOperation operation, Object aggregation) {
        Object actualAggregation = getAggregation(field, operation);
        if (!Objects.equals(aggregation, actualAggregation)) {
            failWithMessage("Expected %s for field %s to be %s but was %s", operation, field, aggregation, actualAggregation);
        }
        return this;
    }

    private Object getAggregation(String field, AggregateOperation operation) {
        isNotNull();
        FieldAggregator fieldAggregator = actual.getFieldAggregator();

        if (fieldAggregator != null) {
            Aggregator<?> aggregator = fieldAggregator.getAggregator(field, operation);
            if (aggregator != null) {
                return aggregator.getAggregation();
            } else {
                failWithMessage("No %s aggregator found for %s", operation, field);
            }
        } else {
            failWithMessage("Expected field aggregator to not be null");
        }
        return this;
    }

}
