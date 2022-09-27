package datawave.interceptor;

import datawave.annotation.Required;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

@ExtendWith(EasyMockExtension.class)
public class RequiredInterceptorTest extends EasyMockSupport {
    
    public static class TestClass {
        public void testMethod(@Required("param") String param) {}
        
        public void testValidValudesMethod(@Required(value = "param", validValues = {"foo", "bar"}) String param) {}
        
    }
    
    private RequiredInterceptor interceptor = null;
    @Mock
    private InvocationContext ctx = null;
    private Method method = null;
    
    @BeforeEach
    public void setup() throws Exception {
        interceptor = new RequiredInterceptor();
    }
    
    @Test
    public void testRequiredWithEmptyStringParameter() throws Exception {
        method = TestClass.class.getMethod("testMethod", String.class);
        // Empty String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {""});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        replayAll();
        Assertions.assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verifyAll();
    }
    
    @Test
    public void testRequiredWithNullStringParameter() throws Exception {
        method = TestClass.class.getMethod("testMethod", String.class);
        // Null String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {null});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        replayAll();
        Assertions.assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verifyAll();
    }
    
    @Test
    public void testRequiredWithStringParameter() throws Exception {
        method = TestClass.class.getMethod("testMethod", String.class);
        // String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {"foo"});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.proceed()).andReturn(null);
        replayAll();
        interceptor.checkRequiredParameters(ctx);
        verifyAll();
    }
    
    @Test
    public void testRequiredValidValuesWithInvalidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {"doh"});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        replayAll();
        Assertions.assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verifyAll();
    }
    
    @Test
    public void testRequiredValidValuesWithFirstValidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {"foo"});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.proceed()).andReturn(null);
        replayAll();
        interceptor.checkRequiredParameters(ctx);
        verifyAll();
    }
    
    @Test
    public void testRequiredValidValuesWithSecondValidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        EasyMock.expect(ctx.getParameters()).andReturn(new String[] {"bar"});
        EasyMock.expect(ctx.getMethod()).andReturn(method);
        EasyMock.expect(ctx.proceed()).andReturn(null);
        replayAll();
        interceptor.checkRequiredParameters(ctx);
        verifyAll();
    }
    
}
