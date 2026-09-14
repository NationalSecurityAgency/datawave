package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

/**
 * A generator for a linear id distribution
 * <p>
 * The offset is added to each generated id, so an offset of 1 produces <code>2, 3, 4, ...</code>
 */
public class SequentialEventIdGenerator extends AbstractEventIdGenerator {

    public static EventIdGenerator create() {
        return new SequentialEventIdGenerator();
    }

    private SequentialEventIdGenerator() {
        // no-op
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        List<Integer> frequencies = new ArrayList<>();
        for (long i = 1; i <= bound && frequencies.size() < count; i++) {
            long eventId = i + offset;
            if (!fitsInInt(eventId)) {
                break;
            }

            if (eventId <= bound) {
                frequencies.add((int) eventId);
            }
        }
        return frequencies;
    }
}
