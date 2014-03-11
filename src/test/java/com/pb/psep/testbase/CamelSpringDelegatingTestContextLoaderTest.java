package com.pb.psep.testbase;


import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
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
import java.util.Date;

/**
 * @author: Andrzej Gdula
 * @created: 11/15/2013 15:08
 * @version: 1.0
 */
@RunWith(org.apache.camel.test.spring.CamelSpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {CamelSpringDelegatingTestContextLoaderTest.TestConfig.class},
        // Since Camel 2.11.0
        loader = CamelSpringDelegatingTestContextLoader.class
)
@MockEndpoints
public class CamelSpringDelegatingTestContextLoaderTest {
    @EndpointInject(uri = "mock:direct:testSize")
    protected MockEndpoint endEndpoint;

    @EndpointInject(uri = "mock:direct:error")
    protected MockEndpoint errorEndpoint;

    @Produce(uri = "direct:test")
    protected ProducerTemplate testProducer;


    static final int FILESINIMPORT = 5;

    @Configuration
    public static class TestConfig extends SingleRouteCamelConfiguration {
        @Bean
        @Override
        public RouteBuilder route() {
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("file:./TempData/in/?recursive=true")
                            .to("mock:direct:testSize")
                            .to("file:./TempData/out/?tempFileName=${file:onlyname}.tmp");

                }
            };
        }
    }

    @Test
    public void testRoute() throws InterruptedException {
        // Given
        endEndpoint.expectedMessageCount(FILESINIMPORT*3);
        // Then
        endEndpoint.assertIsSatisfied();
        org.junit.Assert.assertFalse(new File("./TempData/out/DC01/ARCHIVE/DC01/ARCHIVE").exists());
    }


    public static final String YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";

    @BeforeClass
    static public void setup() throws Exception {
        int fileCounter = 0;
        String[] importDirs = {"DC01", "DC02", "DC03"};
        Date date = DateUtils.parseDate("2012-01-01 12:30", YYYY_MM_DD_HH_MM);
        Date[] dates = new Date[FILESINIMPORT * importDirs.length];

        for (int i = 0; i < FILESINIMPORT * importDirs.length; i++) {
            dates[i] = DateUtils.addMinutes(date, i);
        }

        int dateCounter = 0;
        for (int i = 0; i < FILESINIMPORT; i++) {

            for (String dc : importDirs) {

                File archiveDir = new File("./TempData/in/" + dc + "/ARCHIVE");
                Date fileDate = dates[dateCounter++];
                archiveDir.mkdirs();
                File mrdfArch = new File(archiveDir.getPath() + "/" + String.format("%010d_%03d", ++fileCounter, i).replace("_", "."));
                FileUtils.write(mrdfArch, DateFormatUtils.format(fileDate, YYYY_MM_DD_HH_MM) + " => " + mrdfArch.getAbsolutePath());
                mrdfArch.setLastModified(fileDate.getTime());
            }
        }
    }
}
