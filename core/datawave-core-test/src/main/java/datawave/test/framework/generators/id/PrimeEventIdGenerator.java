package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.numbers.primes.Primes;

/**
 *
 */
public class PrimeEventIdGenerator extends AbstractEventIdGenerator {

    public static EventIdGenerator create() {
        return new PrimeEventIdGenerator();
    }

    private PrimeEventIdGenerator() {
        // no-op
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        List<Integer> eventIds = new ArrayList<>();
        for (long i = 1; i <= bound; i++) {
            if (Primes.isPrime((int) i)) {
                long eventId = i + offset;
                if (!fitsInInt(eventId)) {
                    break;
                }

                if (eventId <= bound) {
                    eventIds.add((int) eventId);
                } else {
                    break;
                }
            }
            if (eventIds.size() >= count) {
                break;
            }
        }
        return eventIds;
    }
}
