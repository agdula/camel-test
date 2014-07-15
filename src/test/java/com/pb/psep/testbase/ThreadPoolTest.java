package com.pb.psep.testbase;


import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.apache.camel.test.spring.CamelSpringDelegatingTestContextLoader;
import org.apache.camel.test.spring.MockEndpoints;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import java.io.File;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test that demonstrates a problem of when
 *       onException(SomeException.class).maximumRedeliveries(-1)
 *     and
 *       scheduledExecutorService=poolOfThreads(X)
 *
 * When there are lots of routes using single threadPool and there happen to be more than X exceptions
 * then thread pool gets hogged and other handlers are unable to process because therad pool is stucked
 * on org.apache.camel.processor.RedeliveryPolicy.sleep(RedeliveryPolicy.java:188)
 *
 *   If camel would instead of waiting schedule retry then it would probably solve the problem.
 *
 *
 *
 * @author: Andrzej Gdula
 * @created: 11/15/2013 15:08
 * @version: 1.0
 */
@RunWith(org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {ThreadPoolTest.TestConfig.class},
        // Since Camel 2.11.0
        loader = CamelSpringDelegatingTestContextLoader.class
)
@MockEndpoints
public class ThreadPoolTest {

    public static final int FILE_IN_SUBDIR = 10;
    private static String[] SUBDIRECTORIES = new String[]{"dir1", "dir2", "dir3", "dir4"};

    @EndpointInject(uri = "mock:direct:testSize")
    protected MockEndpoint endEndpoint;
    @Autowired
    protected ModelCamelContext context;
    @BeforeClass
    static public void setup() throws Exception {

        FileUtils.deleteDirectory(new File("./TempData"));
        FileUtils.forceMkdir(new File("./TempData"));

        for (int i = 0; i < FILE_IN_SUBDIR; i++) {
            for (String subdirectory : SUBDIRECTORIES) {
                FileUtils.write(new File("./TempData/"+subdirectory+"/in/"+subdirectory+"-"+i+".txt"),"hello from "+subdirectory+i);
            }
        }
    }

    public static int numberOfThreads = 1;

    @Configuration
    public static class TestConfig extends SingleRouteCamelConfiguration {
        @Bean
        public ScheduledExecutorService dummyPool(){
               return Executors.newScheduledThreadPool(1); // Doesn't work for one thread
//               return Executors.newScheduledThreadPool(2); // Works for two threads

        }   @Bean
        public DummyService dummyService(){
               return new DummyService();
        }
        @Bean
        @Override
        public RouteBuilder route() {
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {

                    // This is what i need because i have to process files one by one
                    onException(RuntimeException.class)
                            .retryAttemptedLogLevel(LoggingLevel.DEBUG)
                            .logRetryAttempted(false)
                            .redeliveryDelay(1000)
                            .maximumRedeliveries(-1)
                            .logRetryStackTrace(false)
                    ;

                    for (String subdirectory : SUBDIRECTORIES) {
                        from("file:./TempData/"+subdirectory+"/in/" +
                                "?sortBy=file:modified" +
                                "&scheduledExecutorService=#dummyPool")
                                .to("bean:dummyService?method=unstableMethod")
                                .to("file:./TempData/" + subdirectory + "/out/?fileExist=Fail")
                                .to("mock:direct:testSize")
                                .routeId(subdirectory)
                                .autoStartup(false)
                        ;
                    }
                }
            };
        }
    }

    public static class DummyService {
         public void unstableMethod(@Header(Exchange.FILE_NAME) String fileName){
             if(fileName.startsWith("dir1-0.txt")){
                 System.out.println(">>"+Thread.currentThread().getName()+": I don't like file "+fileName);
                throw new RuntimeException(">>"+Thread.currentThread().getName()+": I don't like file "+fileName);
             }else{
                System.out.println("File "+fileName+" is ok");
             }
         }
    }

    @Test
    public void oneThread() throws Exception {
        testRoute_body();
    }
    @Test
    public void twoThread() throws Exception {
        testRoute_body();
    }

    public void testRoute_body() throws Exception {


        // Given
        endEndpoint.expectedMessageCount(
           // If there's an exception in one processor i don't want it to process other files before
           // exception is handled manually. Files HAVE to be process in order
           FILE_IN_SUBDIR * ( SUBDIRECTORIES.length -1 )
        );
        // Then
        endEndpoint.setAssertPeriod(1 * 5000 );

        for (String routeId : SUBDIRECTORIES) {
            context.startRoute(routeId);
        }

        endEndpoint.assertIsSatisfied();

        for (String routeId : SUBDIRECTORIES) {
            context.stopRoute(routeId, 1, TimeUnit.MILLISECONDS);
        }


    }

}
