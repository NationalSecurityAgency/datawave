package datawave.query.jexl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.base.Joiner;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

@State(Scope.Benchmark)
public class ParseQueryBenchmark {

    private ASTJexlScript smallScript;
    private ASTJexlScript largeScript;

    private JexlNode ne;
    private JexlNode er;
    private JexlNode nr;
    private JexlNode gt;
    private JexlNode ge;
    private JexlNode lt;
    private JexlNode le;
    private JexlNode contentPhraseFunction;
    private JexlNode contentWithinFunction;
    private JexlNode contentAdjacentFunction;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Setup(Level.Invocation)
    public void setup() {
        smallScript = parse(buildQueryOfSize(1));
        largeScript = parse(buildQueryOfSize(10));

        ne = parse("FIELD1 != 'value1'").jjtGetChild(0);
        er = parse("FIELD1 =~ 'value1'").jjtGetChild(0);
        nr = parse("FIELD1 !~ 'value1'").jjtGetChild(0);
        gt = parse("FIELD1 > 'value1'").jjtGetChild(0);
        ge = parse("FIELD1 >= 'value1'").jjtGetChild(0);
        lt = parse("FIELD1 < 'value1'").jjtGetChild(0);
        le = parse("FIELD1 <= 'value1'").jjtGetChild(0);
        contentPhraseFunction = parse("content:phrase(FIELD1, termOffsetMap, 'value1', 'value2')").jjtGetChild(0);
        contentWithinFunction = parse("content:within(FIELD1, 2, termOffsetMap, 'value1', 'value2')").jjtGetChild(0);
        contentAdjacentFunction = parse("content:within(FIELD1, termOffsetMap, 'value1', 'value2')").jjtGetChild(0);
    }

    // === small query ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void smallQueryThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(smallScript);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void smallQueryAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(smallScript);
        blackhole.consume(built);
    }

    // === large query ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void largeQueryThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(largeScript);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void largeQueryAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(largeScript);
        blackhole.consume(built);
    }

    // === not equals ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void notEqualsThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(ne);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void notEqualsAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(ne);
        blackhole.consume(built);
    }

    // === regex equals ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void regexEqualsThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(er);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void regexEqualsAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(er);
        blackhole.consume(built);
    }

    // === regex not equals ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void regexNotEqualsThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(nr);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void regexNotEqualsAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(nr);
        blackhole.consume(built);
    }

    // === greater than ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void greaterThanThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(gt);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void greaterThanAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(gt);
        blackhole.consume(built);
    }

    // === greater than equals ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void greaterThanEqualsThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(ge);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void greaterThanEqualsAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(ge);
        blackhole.consume(built);
    }

    // === less than ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void lessThanThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(lt);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void lessThanAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(lt);
        blackhole.consume(built);
    }

    // === less than equals ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void lessThanEqualsThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(le);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void lessThanEqualsAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(le);
        blackhole.consume(built);
    }

    // === content phrase function ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void contentPhraseFunctionThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentPhraseFunction);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void contentPhraseFunctionAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentPhraseFunction);
        blackhole.consume(built);
    }

    // === content within function ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void contentWithinFunctionThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentWithinFunction);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void contentWithinFunctionAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentWithinFunction);
        blackhole.consume(built);
    }

    // === contentAdjacentFunction ===

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void contentAdjacentFunctionThroughput(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentAdjacentFunction);
        blackhole.consume(built);
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void contentAdjacentFunctionAverage(Blackhole blackhole) {
        String built = JexlStringBuildingVisitor.buildQuery(contentAdjacentFunction);
        blackhole.consume(built);
    }

    public String buildQueryOfSize(int size) {
        List<String> terms = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            terms.add("FIELD" + i + " == 'value" + i + "'");
        }
        return Joiner.on(" || ").join(terms);
    }

    public ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
