package datawave.microservice.http.converter.protostuff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.google.common.collect.Lists;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.VoidResponse;
import io.protostuff.ProtostuffIOUtil;

@EnableWebMvc
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"converterTest", "permitAllWebTest"})
public class ProtostuffHttpMessageConverterTest {
    
    @Autowired
    private MockMvc mvc;
    
    @Test
    public void testProtostuffResponse() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/vr").accept("application/x-protostuff")).andExpect(status().isOk()).andReturn();
        
        VoidResponse actual = new VoidResponse();
        ProtostuffIOUtil.mergeFrom(mvcResult.getResponse().getContentAsByteArray(), actual, actual.cachedSchema());
        
        QueryException qe = new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS, "This is a test QueryException");
        ArrayList<String> expectedMessages = Lists.newArrayList("This is a test message", "This is another test message");
        ArrayList<QueryExceptionType> expectedExceptions = Lists.newArrayList(
                        new QueryExceptionType(qe.getMessage(), "Exception with no cause caught", qe.getErrorCode()),
                        new QueryExceptionType("This is the cause exception", "java.lang.Exception: This is the cause exception", null),
                        new QueryExceptionType("this is a test exception", "Exception with no cause caught", null));
        
        assertTrue(actual.getHasResults());
        assertTrue(actual.getOperationTimeMS() >= 0);
        assertEquals(expectedMessages, actual.getMessages());
        assertEquals(expectedExceptions, actual.getExceptions());
    }
    
    @RestController
    @RequestMapping(path = "/", produces = {"application/x-protostuff", "application/json"})
    public static class TestController {
        @RequestMapping(path = "/vr")
        public VoidResponse voidResponse() {
            VoidResponse voidResponse = new VoidResponse();
            voidResponse.setHasResults(true);
            voidResponse.setOperationTimeMS(-1);
            voidResponse.addMessage("This is a test message");
            voidResponse.addMessage("This is another test message");
            voidResponse.addException(new Exception("this is a test exception"));
            voidResponse.addException(new Exception("This is the outer exception", new Exception("This is the cause exception")));
            voidResponse.addException(new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS, "This is a test QueryException"));
            return voidResponse;
        }
    }
    
    @Profile("converterTest")
    @Configuration
    @ComponentScan(basePackages = "datawave.microservice")
    public static class SpringBootConfiguration {}
}
