package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

public class ModuloEventIdGenerator extends AbstractEventIdGenerator {

    private final int modulo;

    public static EventIdGenerator create(int modulo) {
        return new ModuloEventIdGenerator(modulo);
    }

    public static EventIdGenerator create(int offset, int modulo) {
        return new ModuloEventIdGenerator(offset, modulo);
    }

    private ModuloEventIdGenerator(int modulo) {
        this(DEFAULT_OFFSET, modulo);
    }

    private ModuloEventIdGenerator(int offset, int modulo) {
        super(offset);
        Preconditions.checkArgument(modulo > 0, "modulo must be greater than 0");
        this.modulo = modulo;
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        List<Integer> frequencies = new ArrayList<>();
        for (long i = 1; i <= bound; i++) {
            if (i % modulo == 0) {
                long freq = i + offset;
                if (!fitsInInt(freq)) {
                    break;
                }

                if (freq <= bound) {
                    frequencies.add((int) freq);
                }

                if (frequencies.size() >= count) {
                    break;
                }
            }
        }
        return frequencies;
    }
}
