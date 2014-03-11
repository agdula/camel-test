package com.pb.psep.testbase;


import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.aggregate.AggregationStrategy;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.apache.camel.test.spring.CamelSpringDelegatingTestContextLoader;
import org.apache.camel.test.spring.MockEndpoints;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author: Andrzej Gdula
 * @created: 11/15/2013 15:08
 * @version: 1.0
 */
@RunWith(org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {AggregatorTest.TestConfig.class},
        // Since Camel 2.11.0
        loader = CamelSpringDelegatingTestContextLoader.class
)
@MockEndpoints
public class AggregatorTest {
    @EndpointInject(uri = "mock:direct:testSize")

    protected MockEndpoint endEndpoint;
    @EndpointInject(uri = "mock:direct:error")
    protected MockEndpoint errorEndpoint;

    @Produce(uri = "direct:test")
    protected ProducerTemplate testProducer;

    @BeforeClass
    static public void setup() throws Exception {

        String[] bodys = {
          "A","B","B","A","C","B","C","C","C","A","A"
        };

        FileUtils.deleteDirectory(new File("./TempData"));
        FileUtils.forceMkdir(new File("./TempData"));

        for (int i = 0; i < bodys.length; i++) {
            FileUtils.write(new File("./TempData/in/"+i+".txt"),""+bodys[i]);
        }



    }

    @Configuration
    public static class TestConfig extends SingleRouteCamelConfiguration {
        @Bean
        @Override
        public RouteBuilder route() {
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("file:./TempData/in/?sortBy=file:modified")
                            .process(new Processor() {
                                String prev = null;
                                @Override
                                public void process(Exchange exchange) throws Exception {
                                    String current = exchange.getIn().getBody(String.class);
                                    if (prev == null || prev.equals(current)) {
                                        exchange.getIn().setHeader("CONTINUE", "true");
                                    }
                                    exchange.getIn().setHeader("PREV", prev);
                                    exchange.getIn().setHeader("CURRENT", current);
                                    System.out.printf("prev:'%s',current:'%s'\n",prev,current);
                                    prev = current;
                                }
                            })
                            .aggregate(constant(true), new AggregationStrategy() {
                                @Override
                                public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
                                    Exchange result = oldExchange;
                                    final String newBody = newExchange.getIn().getBody(String.class);
                                    System.out.println("oldBody=" + (oldExchange == null ? null : oldExchange.getIn().getBody(String.class)));
                                    System.out.println("newBody=" + (newExchange == null ? null : newExchange.getIn().getBody(String.class)));
                                    if (result == null) {
                                        result = newExchange;
                                        newExchange.getIn().setBody(new ArrayList());
                                    } else {
                                        Message in = result.getIn();
                                        List body = in.getBody(List.class);
                                        System.out.println("BODY=" + (body == null ? null : Arrays.deepToString(body.toArray())));
                                        if (body == null) {
                                            in.setBody(new ArrayList());
                                        }
                                    }
                                    result.getIn().getBody(ArrayList.class).add(newBody);

                                    return result;
                                }
                            })
                            .completionPredicate(new Predicate() {
                                @Override
                                public boolean matches(Exchange exchange) {
                                    String aContinue = exchange.getIn().getHeader("CONTINUE", String.class);
                                    String prev = exchange.getIn().getHeader("PREV", String.class);
                                    String current = exchange.getIn().getHeader("CURRENT", String.class);
                                    System.out.printf("prev:'%s',curr:'%s',continue:'%s'\n",prev,current,aContinue);
                                    return "true".equals(aContinue);
                                }
                            })
                            .completionTimeout(5000)
                            .process(new Processor() {
                                @Override
                                public void process(Exchange exchange) throws Exception {
                                    exchange.getIn().setBody(exchange.getIn().getBody(String.class));
                                }
                            })
                            .eagerCheckCompletion()
                            .to("file:./TempData/out/?fileExist=Fail")
                            .to("mock:direct:testSize");

                }
            };
        }
    }

    @Test
    public void testRoute() throws InterruptedException {
        // Given
        endEndpoint.expectedMessageCount(7);
        // Then
        endEndpoint.assertIsSatisfied();

        endEndpoint.setAssertPeriod(1 * 50000 );

        endEndpoint.assertIsSatisfied();
    }

}
