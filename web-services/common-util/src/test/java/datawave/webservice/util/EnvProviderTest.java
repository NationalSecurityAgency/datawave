package datawave.webservice.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class EnvProviderTest {

    @Test
    public void testNormalValue() {
        String normalProperty = "ChangeMe";
        String resolved = EnvProvider.resolve(normalProperty);
        assertEquals(normalProperty, resolved);
    }

    @Test
    public void testTargetNotFound() {
        String target = "env:" + RandomStringUtils.randomAlphanumeric(25);
        String resolved = EnvProvider.resolve(target);
        assertEquals(target, resolved);
    }

    @Test
    public void testNullTarget() {
        assertNull(EnvProvider.resolve(null));
    }
}
