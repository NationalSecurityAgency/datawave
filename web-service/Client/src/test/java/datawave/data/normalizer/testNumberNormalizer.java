package datawave.data.normalizer;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class testNumberNormalizer {
    
    @Test
    public void test1() throws Exception {
        NumberNormalizer nn = new NumberNormalizer();
        
        String foo = nn.normalize("1386195424282");
        String n1 = nn.normalize("1");
        String n2 = nn.normalize("1.00000000");
        
        assertTrue(n1.equalsIgnoreCase(n2));
        
    }
    
    @Test
    public void test2() throws NormalizationException {
        NumberNormalizer nn = new NumberNormalizer();
        
        String n1 = nn.normalize("-1.0");
        String n2 = nn.normalize("1.0");
        
        assertTrue(n1.compareToIgnoreCase(n2) < 0);
        
    }
    
    @Test
    public void test3() throws NormalizationException {
        NumberNormalizer nn = new NumberNormalizer();
        String n1 = nn.normalize("-0.0001");
        String n2 = nn.normalize("0");
        String n3 = nn.normalize("0.00001");
        
        assertTrue((n1.compareToIgnoreCase(n2) < 0) && (n2.compareToIgnoreCase(n3) < 0));
    }
    
    @Test
    public void test4() throws NormalizationException {
        NumberNormalizer nn = new NumberNormalizer();
        String nn1 = nn.normalize(Integer.toString(Integer.MAX_VALUE));
        String nn2 = nn.normalize(Integer.toString(Integer.MAX_VALUE - 1));
        
        assertTrue((nn2.compareToIgnoreCase(nn1) < 0));
        
    }
    
    @Test
    public void test5() throws NormalizationException {
        NumberNormalizer nn = new NumberNormalizer();
        String nn1 = nn.normalize("-0.001");
        String nn2 = nn.normalize("-0.0009");
        String nn3 = nn.normalize("-0.00090");
        
        assertTrue((nn3.equalsIgnoreCase(nn2)) && (nn2.compareToIgnoreCase(nn1) > 0));
        
    }
    
    @Test
    public void test6() throws NormalizationException {
        NumberNormalizer nn = new NumberNormalizer();
        String nn1 = nn.normalize("-0.0");
        String nn2 = nn.normalize("0");
        String nn3 = nn.normalize("0.0");
        
        assertTrue((nn3.equalsIgnoreCase(nn2)) && (nn2.equalsIgnoreCase(nn1)));
        
    }
    
}
