package datawave.test.framework.generators.id;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * A generator that produces every event id matching <code>id % modulo == offset</code>
 * <p>
 * The offset selects a residue class, so one modulo used with every offset from <code>0</code> to <code>modulo - 1</code> partitions the event ids. Modulo 2
 * offset 0 generates <code>2, 4, 6</code> and modulo 2 offset 1 generates <code>1, 3, 5</code>.
 */
public class ModuloEventIdGenerator extends AbstractEventIdGenerator {

    private final int modulo;

    /**
     * Create a generator of the ids divisible by the modulo
     *
     * @param modulo
     *            the modulo
     * @return the generator
     */
    public static EventIdGenerator create(int modulo) {
        return new ModuloEventIdGenerator(modulo);
    }

    /**
     * Create a generator of the ids in a single residue class
     *
     * @param offset
     *            the residue, which must be less than the modulo
     * @param modulo
     *            the modulo
     * @return the generator
     */
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
        checkOffset(offset);
    }

    /**
     * Configure the residue class selected by this generator
     *
     * @param offset
     *            the offset, which must be less than the modulo
     */
    @Override
    public void setOffset(int offset) {
        checkOffset(offset);
        super.setOffset(offset);
    }

    private void checkOffset(int offset) {
        Preconditions.checkArgument(offset >= 0 && offset < modulo, "offset must be at least 0 and less than the modulo");
    }

    @Override
    public List<Integer> generateCountWithinBound(int count, int bound) {
        List<Integer> frequencies = new ArrayList<>();
        for (long i = 1; i <= bound && frequencies.size() < count; i++) {
            if (i % modulo == offset) {
                frequencies.add((int) i);
            }
        }
        return frequencies;
    }
}
