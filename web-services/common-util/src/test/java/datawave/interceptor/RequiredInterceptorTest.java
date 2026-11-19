package datawave.interceptor;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Method;

import javax.interceptor.InvocationContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.annotation.Required;

@ExtendWith(MockitoExtension.class)
public class RequiredInterceptorTest {

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
        doReturn(new String[] {""}).when(ctx).getParameters();
        doReturn(method, method).when(ctx).getMethod();

        assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(2)).getMethod();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

    @Test
    public void testRequiredWithNullStringParameter() throws Exception {
        method = TestClass.class.getMethod("testMethod", String.class);
        // Null String parameter
        doReturn(new String[] {null}).when(ctx).getParameters();
        doReturn(method, method).when(ctx).getMethod();

        assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(2)).getMethod();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

    @Test
    public void testRequiredWithStringParameter() throws Exception {
        method = TestClass.class.getMethod("testMethod", String.class);
        // String parameter
        doReturn(new String[] {"foo"}).when(ctx).getParameters();
        doReturn(method).when(ctx).getMethod();
        doReturn(null).when(ctx).proceed();
        interceptor.checkRequiredParameters(ctx);
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(1)).getMethod();
        verify(ctx, times(1)).proceed();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

    @Test
    public void testRequiredValidValuesWithInvalidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        doReturn(new String[] {"doh"}).when(ctx).getParameters();
        doReturn(method, method).when(ctx).getMethod();

        assertThrows(IllegalArgumentException.class, () -> interceptor.checkRequiredParameters(ctx));
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(2)).getMethod();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

    @Test
    public void testRequiredValidValuesWithFirstValidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        doReturn(new String[] {"foo"}).when(ctx).getParameters();
        doReturn(method).when(ctx).getMethod();
        doReturn(null).when(ctx).proceed();
        interceptor.checkRequiredParameters(ctx);
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(1)).getMethod();
        verify(ctx, times(1)).proceed();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

    @Test
    public void testRequiredValidValuesWithSecondValidValue() throws Exception {
        method = TestClass.class.getMethod("testValidValudesMethod", String.class);
        // String parameter
        doReturn(new String[] {"bar"}).when(ctx).getParameters();
        doReturn(method).when(ctx).getMethod();
        doReturn(null).when(ctx).proceed();
        interceptor.checkRequiredParameters(ctx);
        verify(ctx, times(1)).getParameters();
        verify(ctx, times(1)).getMethod();
        verify(ctx, times(1)).proceed();

        if (ctx != null && mockingDetails(ctx).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(ctx));
        }
    }

}
