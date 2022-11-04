package datawave.query.attributes;

import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;

/**
 *
 */
public class AttributeFactoryTest {
    
    private static final Logger log = Logger.getLogger(AttributeFactoryTest.class);
    
    Collection<Class<?>> one = Sets.newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> two = Sets.newHashSet(NoOpType.class, LcNoDiacriticsType.class);
    Collection<Class<?>> three = Sets.newHashSet(NoOpType.class, LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> four = Sets.newHashSet(LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> five = Sets.newHashSet(NoOpType.class, NumberType.class);
    
    Collection<Class<?>> expectedOne = Sets.newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedTwo = Sets.newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedThree = Sets.newHashSet(NumberType.class);
    Collection<Class<?>> expectedFour = Sets.newHashSet(NumberType.class);
    Collection<Class<?>> expectedFive = Sets.newHashSet(NumberType.class);
    
    @Test
    public void testFindersKeepers() {
        log.debug(AttributeFactory.getKeepers(one));
        log.debug(AttributeFactory.getKeepers(two));
        log.debug(AttributeFactory.getKeepers(three));
        log.debug(AttributeFactory.getKeepers(four));
        log.debug(AttributeFactory.getKeepers(five));
        
        Assertions.assertEquals(AttributeFactory.getKeepers(one), expectedOne);
        Assertions.assertEquals(AttributeFactory.getKeepers(two), expectedTwo);
        Assertions.assertEquals(AttributeFactory.getKeepers(three), expectedThree);
        Assertions.assertEquals(AttributeFactory.getKeepers(four), expectedFour);
        Assertions.assertEquals(AttributeFactory.getKeepers(five), expectedFive);
        
    }
    
}
