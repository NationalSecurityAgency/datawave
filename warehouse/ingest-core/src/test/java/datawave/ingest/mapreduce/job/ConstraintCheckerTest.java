package datawave.ingest.mapreduce.job;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ConstraintCheckerTest {
    
    private ConstraintChecker constraintChecker;
    
    private byte[] emptyViz = new ColumnVisibility().getExpression();
    private byte[] nonemptyViz = new ColumnVisibility("A&B").getExpression();
    
    @Before
    public void setup() {
        Configuration conf = new Configuration();
        conf.set(ConstraintChecker.INITIALIZERS, NonemptyVisibilityConstraint.Initializer.class.getName());
        conf.set(NonemptyVisibilityConstraint.Initializer.TABLE_CONFIG, "eventTable");
        
        constraintChecker = ConstraintChecker.create(conf);
    }
    
    @Test(expected = ConstraintChecker.ConstraintViolationException.class)
    public void shouldFailOnViolatedConstraint() {
        constraintChecker.check(new Text("eventTable"), emptyViz);
    }
    
    @Test
    public void shouldPassOnNotViolatedConstraint() {
        constraintChecker.check(new Text("eventTable"), nonemptyViz);
    }
    
    @Test
    public void shouldPassForNonConfiguredTable() {
        constraintChecker.check(new Text("indexTable"), emptyViz);
    }
    
    @Test
    public void shouldPassIfNotConfigured() {
        Configuration blankConf = new Configuration();
        constraintChecker = ConstraintChecker.create(blankConf);
        
        constraintChecker.check(new Text("eventTable"), emptyViz);
        constraintChecker.check(new Text("eventTable"), nonemptyViz);
    }
    
    @Test
    public void shouldNotBeConfiguredIfNoInitializers() {
        constraintChecker = ConstraintChecker.create(new Configuration());
        assertThat(constraintChecker.isConfigured(), is(false));
    }
}
