package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

/**
 * A generator that produces the squares, <code>1, 4, 9, 16, 25, 36, ...</code>
 * <p>
 * The offset is added to each generated id, so an offset of 1 produces <code>2, 5, 10, ...</code>
 */
public class SquaresEventIdGenerator extends AbstractEventIdGenerator {

    public static EventIdGenerator create() {
        return new SquaresEventIdGenerator();
    }

    private SquaresEventIdGenerator() {
        // no-op
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        List<Integer> eventIds = new ArrayList<>();
        for (long i = 1; i <= bound && eventIds.size() < count; i++) {
            long eventId = offset + (i * i);
            if (!fitsInInt(eventId) || eventId > bound) {
                break;
            }

            eventIds.add((int) eventId);
        }
        return eventIds;
    }
}
