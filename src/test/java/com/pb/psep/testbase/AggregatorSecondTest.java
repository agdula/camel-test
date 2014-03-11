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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: Andrzej Gdula
 * @created: 11/15/2013 15:08
 * @version: 1.0
 */
@RunWith(org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {AggregatorSecondTest.TestConfig.class},
        // Since Camel 2.11.0
        loader = CamelSpringDelegatingTestContextLoader.class
)
@MockEndpoints
public class AggregatorSecondTest {
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
                                String prev = null;
                                @Override
                                public boolean matches(Exchange exchange) {
                                    boolean cont = false;
                                    String current = exchange.getIn().getBody(String.class);
                                    if (prev == null || prev.equals(current)) {
                                        cont = true;
                                    }
                                    exchange.getIn().setHeader("PREV", prev);
                                    exchange.getIn().setHeader("CURRENT", current);
                                    System.out.printf("prev:'%s',current:'%s',continue'%b'\n",prev,current,cont);
                                    prev = current;
                                    return cont;
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
