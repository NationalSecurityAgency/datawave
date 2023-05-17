package datawave.common;

import datawave.common.classloader.ErrorSafeVfsClassLoaderFactory;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.function.Predicate;

public class ErrorSafeDelegatingClassLoaderTest {

    private static File tempFile;

    private static AccumuloConfiguration conf = new AccumuloConfiguration() {
        @Override
        public String get(Property property) {
            switch (property.getKey()) {
                case "general.context.class.loader.factory":
                    return ErrorSafeVfsClassLoaderFactory.class.getName();
                case "general.server.threadpool.size":
                    return "2";
                case "general.server.simpletimer.threadpool.size":
                    return "2";
            }
            return null;
        }

        @Override
        public void getProperties(Map<String, String> props, Predicate<String> filter) {
            if (filter.test("general.vfs.context.classpath.safecontext")) {
                URL url = getClass().getResource("/test.jar");
                props.put("general.vfs.context.classpath.safecontext", url.toString());
                try {
                    File file = copyToTempFile(url.toURI());
                    props.put("general.vfs.context.classpath.badcontext", file.toURI().toURL().toString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private File copyToTempFile(URI uri) {
            try {
                tempFile = File.createTempFile("temp", ".jar");
                tempFile.deleteOnExit();
                FileUtils.copyFile(new File(uri), tempFile);
                return tempFile;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isPropertySet(Property prop) {
            return get(prop) != null;
        }
    };

    static {
        ClassLoaderUtil.initContextFactory(conf);
        try {
            Field field = ClassLoaderUtil.class.getDeclaredField("FACTORY");
            field.setAccessible(true);
            ErrorSafeVfsClassLoaderFactory factory = (ErrorSafeVfsClassLoaderFactory) (field.get(ClassLoaderUtil.class));
            factory.setConfiguration(conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestHappyPath() throws ClassNotFoundException {
        ClassLoader loader = ClassLoaderUtil.getClassLoader("safecontext");
        try {
            Class clazz = loader.loadClass("datawave.TestClass");
            Assert.assertNotNull(clazz);
        } catch (ClassNotFoundException e) {
            Assert.fail("Expected to find test.TestClass");
        }
        try {
            Class clazz = loader.loadClass("bogus.TestClass");
           Assert.fail("Expected a class not found exception");
        } catch (ClassNotFoundException e) {
            // expected
        }
    }

    @Test
    public void TestErrorPath() throws URISyntaxException, IOException {
        ClassLoader loader = ClassLoaderUtil.getClassLoader("badcontext");
        try {
            Class clazz = loader.loadClass("datawave.TestClass");
            Assert.assertNotNull(clazz);
        } catch (ClassNotFoundException e) {
            Assert.fail("Expected to find test.TestClass");
        }

        ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[1024]);
        FileUtils.copyToFile(inputStream, tempFile);

        try {
            Class clazz = loader.loadClass("datawave.FailureClass");
            Assert.fail("Expected a class not found exception");
        } catch (ClassNotFoundException e) {
            // expected
        }
    }
}
