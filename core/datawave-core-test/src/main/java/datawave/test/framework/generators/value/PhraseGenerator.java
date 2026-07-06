package datawave.test.framework.generators.value;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;

public class PhraseGenerator implements ValueGenerator<String> {

    public static ValueGenerator<String> create() {
        return new PhraseGenerator();
    }

    private PhraseGenerator() {
        // enforce static access
    }

    @Override
    public String next() {
        List<String> words = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            words.add(RandomStringUtils.secure().nextAlphabetic(5));
        }
        return String.join(" ", words);
    }
}
