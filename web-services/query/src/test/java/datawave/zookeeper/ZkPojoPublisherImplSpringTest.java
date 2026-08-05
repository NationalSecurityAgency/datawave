package datawave.zookeeper;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(locations = "classpath:TestZkObjectPublisherFactory.xml")
class ZkPojoPublisherImplSpringTest {

    private TestingServer server;

    @Autowired
    private ZkPojoPublisherImpl publisher;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @Test
    void testCreation() {
        assertNotNull(publisher);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }
}
