package datawave.query.tables.facets;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.*;

public class MergedReadAheadTest {
    @Test
    public void testMergedReadAheadNonStreaming() {
        testMergedReadAhead(Arrays.asList("A", "B", "C", "D", "E"), false, null, null);
    }
    
    @Test
    public void testMergedReadAheadStreaming() {
        testMergedReadAhead(Arrays.asList("A", "B", "C", "D", "E"), false, null, null);
    }
    
    @Test
    public void testMergedReadAheadWithFunction() {
        testMergedReadAhead(Arrays.asList("a", "b", "c", "d", "e"), false, lowercaseFunction, null);
    }
    
    @Test
    public void testMergedReadAheadWithPredicate() {
        testMergedReadAhead(Arrays.asList("B", "C", "D"), false, null, Collections.singletonList(vowelFilterPredicate));
    }
    
    @Test
    public void testMergedReadAheadWithFunctionAndPredicate() {
        testMergedReadAhead(Arrays.asList("b", "c", "d"), false, lowercaseFunction, Collections.singletonList(vowelFilterPredicate));
    }
    
    @Test
    public void testMergedReadAheadFunctionAndPredicateOrdering() {
        // predicates are applied after functions, so the uppercase predicate filters nothing
        testMergedReadAhead(Arrays.asList("a", "b", "c", "d", "e"), false, lowercaseFunction, Collections.singletonList(uppercaseVowelFilterPredicate));
    }
    
    public void testMergedReadAhead(List<String> expected, boolean streaming, Function<String,String> functionalMerge, List<Predicate<String>> filters) {
        final List<String> input = Arrays.asList("A", "B", "C", "D", "E");
        final List<String> output = new ArrayList<>();
        
        MergedReadAhead<String> mra = new MergedReadAhead<>(streaming, input.iterator(), functionalMerge, filters);
        while (mra.hasNext()) {
            output.add(mra.next());
        }
        
        assertArrayEquals("Expected input and output arrays to be equal (streaming=" + streaming + ")", expected.toArray(), output.toArray());
    }
    
    static final Function<String,String> lowercaseFunction = new Function<String,String>() {
        @Nullable
        @Override
        public String apply(@Nullable String s) {
            if (s == null)
                return null;
            return s.toLowerCase(Locale.ENGLISH);
        }
    };
    
    static final Predicate<String> vowelFilterPredicate = new Predicate<String>() {
        final List<String> vowels = Arrays.asList("A", "E", "I", "O", "U", "a", "e", "i", "o", "u");
        
        @Override
        public boolean apply(@Nullable String s) {
            for (String v : vowels) {
                if (s.startsWith(v)) {
                    return false;
                }
            }
            return true;
        }
    };
    
    static final Predicate<String> uppercaseVowelFilterPredicate = new Predicate<String>() {
        final List<String> vowels = Arrays.asList("A", "E", "I", "O", "U");
        
        @Override
        public boolean apply(@Nullable String s) {
            for (String v : vowels) {
                if (s.startsWith(v)) {
                    return false;
                }
            }
            return true;
        }
    };
    
}
