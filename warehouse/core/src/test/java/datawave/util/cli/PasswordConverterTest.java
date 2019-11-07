package datawave.util.cli;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class PasswordConverterTest {
    
    private String[] argv;
    private Password password;
    
    private class Password {
        @Parameter(names = "--password", converter = PasswordConverter.class)
        String password;
    }
    
    @Before
    public void setup() {
        argv = new String[] {"--password", ""};
        password = new Password();
    }
    
    @Test
    public void testConvertEnvPrefixed() {
        // just use the first thing that shows up in the environment
        String name = System.getenv().keySet().iterator().next();
        argv[1] = "env:" + name;
        new JCommander(password).parse(argv);
        assertThat(password.password, is(equalTo(System.getenv(name))));
    }
    
    @Test
    public void testConvertEnvPrefixedEmpty() {
        // prefix with no var defined should return null
        argv[1] = "env:";
        new JCommander(password).parse(argv);
        assertThat(password.password, is(nullValue()));
    }
    
    @Test
    public void testConvertEnvPrefixedUndefinedEnvironmentVar() {
        // prefix with env var not defined should return null
        String name = "HighlyUnlikelyThisWillBeDefined";
        // make sure it is not defined
        assertThat("Expected " + name + " to not be defined but it was!", System.getenv(name), is(nullValue()));
        
        argv[1] = "env:" + name;
        
        new JCommander(password).parse(argv);
        assertThat(password.password, is(nullValue()));
    }
    
    @Test
    public void testConvertNonEnvPrefixed() {
        String expected = "behavior";
        argv[1] = expected;
        new JCommander(password).parse(argv);
        assertThat(password.password, is(equalTo(expected)));
    }
    
}
