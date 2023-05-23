package datawave.query.attributes;

import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

/**
 *
 */
public class AttributeFactoryTest {
    
    private static final Logger log = Logger.getLogger(AttributeFactoryTest.class);
    
    Collection<Class<?>> one = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> two = Sets.<Class<?>> newHashSet(NoOpType.class, LcNoDiacriticsType.class);
    Collection<Class<?>> three = Sets.<Class<?>> newHashSet(NoOpType.class, LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> four = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> five = Sets.<Class<?>> newHashSet(NoOpType.class, NumberType.class);
    
    Collection<Class<?>> expectedOne = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedTwo = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedThree = Sets.<Class<?>> newHashSet(NumberType.class);
    Collection<Class<?>> expectedFour = Sets.<Class<?>> newHashSet(NumberType.class);
    Collection<Class<?>> expectedFive = Sets.<Class<?>> newHashSet(NumberType.class);
    
    @Test
    public void testFindersKeepers() {
        log.debug(AttributeFactory.getKeepers(one));
        log.debug(AttributeFactory.getKeepers(two));
        log.debug(AttributeFactory.getKeepers(three));
        log.debug(AttributeFactory.getKeepers(four));
        log.debug(AttributeFactory.getKeepers(five));
        
        Assert.assertEquals(AttributeFactory.getKeepers(one), expectedOne);
        Assert.assertEquals(AttributeFactory.getKeepers(two), expectedTwo);
        Assert.assertEquals(AttributeFactory.getKeepers(three), expectedThree);
        Assert.assertEquals(AttributeFactory.getKeepers(four), expectedFour);
        Assert.assertEquals(AttributeFactory.getKeepers(five), expectedFive);
        
    }
    
}
