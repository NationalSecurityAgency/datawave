package datawave.data.normalizer.regex.visitor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexParser;

class NonEncodedNumbersCheckerTest {
    
    @Test
    void testSingleSimpleNumber() {
        assertHasNoNonEncodedNumbers(encodeSimpleNumbers("123"));
    }
    
    @Test
    void testSingleNonSimpleNumber() {
        assertHasNonEncodedNumbers(encodeSimpleNumbers("2342.*"));
    }
    
    @Test
    void testAlternatedSimpleNumbers() {
        assertHasNoNonEncodedNumbers(encodeSimpleNumbers("234|-45345"));
    }
    
    @Test
    void testAlternatedNonSimpleNumbers() {
        assertHasNonEncodedNumbers(encodeSimpleNumbers("234.*|65{3}"));
    }
    
    @Test
    void testAlternatedSimpleNumberAndNonSimpleNumber() {
        assertHasNonEncodedNumbers(encodeSimpleNumbers("324.*|345"));
    }
    
    private Node encodeSimpleNumbers(String pattern) {
        return SimpleNumberEncoder.encode(RegexParser.parse(pattern));
    }
    
    private void assertHasNonEncodedNumbers(Node node) {
        assertThat(NonEncodedNumbersChecker.check(node)).isTrue();
    }
    
    private void assertHasNoNonEncodedNumbers(Node node) {
        assertThat(NonEncodedNumbersChecker.check(node)).isFalse();
    }
}
