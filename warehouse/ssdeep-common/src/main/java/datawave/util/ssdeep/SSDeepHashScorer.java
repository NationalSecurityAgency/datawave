package datawave.util.ssdeep;

public interface SSDeepHashScorer {
    public int apply(SSDeepHash signature1, SSDeepHash signature2);
}
