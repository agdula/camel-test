package com.pb.psep.testbase;
/****************************************************************************/
/*                                                                          */
/*  NOTICE                                                                  */
/*                                                                          */
/* Confidential, unpublished property of Pitney Bowes, Inc.                 */
/* Use and distribution limited solely to authorized personnel.             */
/*                                                                          */
/* The use, disclosure, reproduction, modification, transfer, or            */
/* transmittal of this work for any purpose in any form or by               */
/* any means without the written permission of Pitney Bowes                 */
/* is strictly prohibited.                                                  */
/*                                                                          */
/* Copyright 2013 Pitney Bowes, Inc.                                     */
/* All Rights Reserved.                                                     */
/*                                                                          */
/****************************************************************************/

import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.apache.camel.spring.javaconfig.test.JavaConfigContextLoader;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * @author: Andrzej Gdula
 * @created: 11/20/2013 17:25
 * @version: 1.0
 */
@ContextConfiguration(
        locations = "com.pb.psep.testbase.FilterTest$ContextConfig",
        loader = JavaConfigContextLoader.class)
public class FilterTest extends AbstractJUnit4SpringContextTests {

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @DirtiesContext
    @Test
    public void testSendMatchingMessage() throws Exception {
        String expectedBody = "<matched/>";

        resultEndpoint.expectedBodiesReceived(expectedBody);

        template.sendBodyAndHeader(expectedBody, "foo", "bar");

        resultEndpoint.assertIsSatisfied();
    }

    @DirtiesContext
    @Test
    public void testSendNotMatchingMessage() throws Exception {
        resultEndpoint.expectedMessageCount(0);

        template.sendBodyAndHeader("<notMatched/>", "foo", "notMatchedHeaderValue");

        resultEndpoint.assertIsSatisfied();
    }

    @Configuration
    public static class ContextConfig extends SingleRouteCamelConfiguration {
        @Bean
        public RouteBuilder route() {
            return new RouteBuilder() {
                public void configure() {
                    from("direct:start").filter(header("foo").isEqualTo("bar")).to("mock:result");
                }
            };
        }
    }
}
