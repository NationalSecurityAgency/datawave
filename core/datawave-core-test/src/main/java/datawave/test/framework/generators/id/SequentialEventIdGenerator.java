package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

/**
 * A generator for a linear id distribution
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
        for (long i = 1; i <= bound; i++) {
            long eventId = i + offset;
            if (!fitsInInt(eventId)) {
                break;
            }

            if (eventId <= bound) {
                frequencies.add((int) eventId);
            }
            if (frequencies.size() == count) {
                break;
            }
        }
        return frequencies;
    }
}
