package datawave.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Simply factory using reflection (i.e. Class.forname() and Constructor.newInstance) to create the specified object.
 */
public class ObjectFactory {
    private static final Logger logger = Logger.getLogger(ObjectFactory.class);

    /**
     * Take away the public constructor
     */
    private ObjectFactory() {}

    /**
     * Create an object from it's classname using args for arguments
     *
     * @param className
     *            the string classname to get a new instance of
     * @param args
     *            the arguments to a public constructor of classname
     * @return the newly instantiated object
     */
    public static Object create(String className, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug("ObjectFactory.create1(" + className + "," + Arrays.toString(args) + ")");
        }
        try {
            Class<?> clazz = Class.forName(className);
            List<Class<?>> types = new ArrayList<>();
            for (Object o : args) {
                if (o == null) {
                    types.add(null);
                } else {
                    types.add(o.getClass());
                }
            }

            Constructor<?> constructor = null;

            // Look for exact match
            try {
                constructor = clazz.getConstructor(types.toArray(new Class[0]));
            } catch (NoSuchMethodException e) {
                logger.debug("No constructor for [" + className + "] in ObjectFactory.create())");
            }

            // Look for assignable match if nothing exact was found
            if (constructor == null) {
                Constructor<?>[] constructors = clazz.getConstructors();
                for (int i = 0; i < constructors.length && constructor == null; i++) {
                    Constructor<?> c = constructors[i];
                    Class<?> ctypes[] = c.getParameterTypes();

                    if (logger.isDebugEnabled()) {
                        logger.debug("Checking:" + className + ", " + clazz);
                        logger.debug("   types   :" + Arrays.toString(ctypes));
                        logger.debug("   numParms:" + ctypes.length + " =? " + types.size());
                    }

                    if (ctypes.length != types.size()) {
                        logger.debug("    not equal:");
                        constructor = null;
                        continue;
                    }
                    constructor = c;
                    for (int j = 0; j < types.size(); j++) {
                        Class<?> a = ctypes[j];
                        Class<?> b = types.get(j);
                        if (a == null || b == null) {
                            continue;
                        }

                        if (a.isAssignableFrom(b)) {
                            logger.debug("   param=" + a + "  assignable " + b);
                        } else {
                            b = getPrim(b);
                            if (a.isAssignableFrom(b)) {
                                logger.debug("   param:" + a + "  assignable " + b);
                            } else {
                                logger.debug("   param:" + a + " !assignable " + b);
                                constructor = null;
                                break;
                            }
                        }
                    }
                }
            }
            Object newObject = null;
            if (constructor == null) {
                logger.info("Failed to find constructor for args(" + args.length + ") types(" + types.size() + ") : " + types);
            } else {
                newObject = constructor.newInstance(args);
            }
            return newObject;
        } catch (ClassNotFoundException e1) {
            logger.error("Could not find class", e1);
            throw new Error(e1);
        } catch (InstantiationException e3) {
            logger.error("Could not instantiate", e3);
            throw new Error(e3);
        } catch (IllegalAccessException e4) {
            logger.error("Could not call constructor", e4);
            throw new Error(e4);
        } catch (InvocationTargetException e5) {
            logger.error("Constructor failed", e5);
            throw new Error(e5);
        } catch (Throwable t) {
            logger.error("Problem in factory", t);
            throw new Error(t);
        }
    }

    private static Map<Class<?>,Class<?>> PrimClass = new HashMap<>();
    static {
        PrimClass.put(Integer.class, Integer.TYPE);
        PrimClass.put(Boolean.class, Boolean.TYPE);
        PrimClass.put(Float.class, Float.TYPE);
        PrimClass.put(Character.class, Character.TYPE);
        PrimClass.put(Long.class, Long.TYPE);
        PrimClass.put(Double.class, Double.TYPE);
        PrimClass.put(Byte.class, Byte.TYPE);
    }

    /**
     * Return primitive for a Primitive wrapper, i.e. int --&gt; Integer
     *
     * @return the class requested
     */
    public static Class<?> getPrim(Class<?> clazz) {
        Class<?> newClass = (Class<?>) PrimClass.get(clazz);
        if (newClass == null)
            newClass = clazz;
        return newClass;
    }

    /**
     * Create an object of the type specified using a no-arg constructor
     *
     * @param className
     *            the string class name to instantiate
     * @return the newly instantiated object
     */
    public static Object create(String className) {
        return create(className, new Object[] {});
    }

    /**
     * Create an object
     *
     * @param className
     *            the string classname to get a new instance of
     * @param args
     *            the arguments to a public constructor of classname
     * @param location
     *            name used for debug logging, if enabled
     * @return the newly instantiated object
     */
    public static Object create(String className, Object[] args, String location) {
        if (logger.isDebugEnabled()) {
            logger.debug("ObjectFactory.create(" + className + "," + Arrays.toString(args) + "," + location + ")");
        }
        return create(className, args);
    }
}
