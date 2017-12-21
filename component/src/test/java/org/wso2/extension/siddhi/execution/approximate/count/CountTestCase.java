/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.approximate.count;


import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class CountTestCase {
    private static final Logger LOG = Logger.getLogger(CountTestCase.class);
    private final int totalEventsSent = 2000;
    private final int noOfUniqueEvents = 100;

    private AtomicInteger totalEventsArrived;
    private AtomicInteger validEvents;
    private AtomicBoolean eventArrived;
    private AtomicLong exactCount;
    private long lowerBound;
    private long upperBound;

    @BeforeMethod
    public void init() {
        totalEventsArrived = new AtomicInteger(0);
        validEvents = new AtomicInteger(0);
        eventArrived = new AtomicBoolean(false);
        exactCount = new AtomicLong(0);
    }

    @Test
    public void testApproximateCount_1() throws InterruptedException {
        final int windowLength = 1000;
        final double confidence = 0.75;
        final double relativeError = 0.005;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "a specified relative error(" + relativeError + ") and a confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalEventsArrived.incrementAndGet();

                    if (totalEventsArrived.get() < windowLength) {
                        exactCount.set((totalEventsArrived.get() / noOfUniqueEvents) + 1);
                    } else {
                        exactCount.set(windowLength / noOfUniqueEvents);
                    }

                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);

                    if (exactCount.get() >= lowerBound && exactCount.get() <= upperBound) {
                        validEvents.incrementAndGet();
                    }
                }
                eventArrived.set(true);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalEventsSent; noOfEvents++) {
            inputHandler.send(new Object[]{noOfEvents % noOfUniqueEvents});
        }

        SiddhiTestHelper.waitForEvents(200, totalEventsSent, totalEventsArrived, 60000);
        Assert.assertEquals(totalEventsSent, totalEventsArrived.get());
        Assert.assertTrue(eventArrived.get());
        Assert.assertTrue((double) validEvents.get() / totalEventsArrived.get() >= confidence);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"testApproximateCount_1"})
    public void testApproximateCount_2() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to check the number of parameters " +
                "passed to the count functions are not 1 or 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, 0.04) " +
                "select * " +
                "insert into outputStream;");

        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("1 or 3 attributes are expected but 2 attributes" +
                    " are found inside the count function"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_2"})
    public void testApproximateCount_3() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 2nd parameter inside count function is a constant");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, number, 0.96) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside count function" +
                    " - 'relative.error' has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_3"})
    public void testApproximateCount_4() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 2nd parameter inside " +
                "count function is a double or float");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, '0.01', 0.03) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside count function" +
                    " - 'relative.error' should be of type Double or Float but found STRING"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_4"})
    public void testApproximateCount_5() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 2nd parameter " +
                "inside count function is in (0, 1) range");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, 1.01, 0.9) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside count function" +
                    " - 'relative.error' must be in the range of (0, 1) but found 1.01"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_5"})
    public void testApproximateCount_6() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 3rd parameter inside count function is a constant");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, 0.01, number) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside count function" +
                    " - 'confidence' has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_6"})
    public void testApproximateCount_7() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 3rd parameter " +
                "inside count function is a double or float");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, 0.04, '0.6') " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside count function" +
                    " - 'confidence' should be of type Double or Float but found STRING"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_7"})
    public void testApproximateCount_8() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 3rd parameter " +
                "inside count function is in (0, 1) range");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number,0.04, -1.01) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside count function" +
                    " - 'confidence' must be in the range of (0, 1) but found -1.01"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_8"})
    public void testApproximateCount_9() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Count Test Case - to validate the 1st parameter " +
                "inside count function is a variable");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(12, 0.04, 0.99) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 1st parameter inside count function - " +
                    "'value' has to be a variable but found" +
                    " org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test(dependsOnMethods = {"testApproximateCount_9"})
    public void testApproximateCount_10() throws InterruptedException {
        final int windowLength = 1000;
        final double confidence = 0.99;
        final double relativeError = 0.01;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "default relative error(" + relativeError + ") and confidence(" + confidence + ") - int input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalEventsArrived.incrementAndGet();

                    if (totalEventsArrived.get() < windowLength) {
                        exactCount.set((totalEventsArrived.get() / noOfUniqueEvents) + 1);
                    } else {
                        exactCount.set(windowLength / noOfUniqueEvents);
                    }

                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);

                    if (exactCount.get() >= lowerBound && exactCount.get() <= upperBound) {
                        validEvents.incrementAndGet();
                    }
                }
                eventArrived.set(true);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalEventsSent; noOfEvents++) {
            inputHandler.send(new Object[]{noOfEvents % noOfUniqueEvents});
        }

        SiddhiTestHelper.waitForEvents(200, totalEventsSent, totalEventsArrived, 60000);
        Assert.assertEquals(totalEventsSent, totalEventsArrived.get());
        Assert.assertTrue(eventArrived.get());
        Assert.assertTrue((double) validEvents.get() / totalEventsArrived.get() >= confidence);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"testApproximateCount_10"})
    public void testApproximateCount_11() throws InterruptedException {
        final int windowLength = 1000;
        final double confidence = 0.99;
        final double relativeError = 0.01;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "default relative error(" + relativeError + ") and confidence(" + confidence + ") - string input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalEventsArrived.incrementAndGet();

                    if (totalEventsArrived.get() < windowLength) {
                        exactCount.set((totalEventsArrived.get() / noOfUniqueEvents) + 1);
                    } else {
                        exactCount.set(windowLength / noOfUniqueEvents);
                    }

                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);

                    if (exactCount.get() >= lowerBound && exactCount.get() <= upperBound) {
                        validEvents.incrementAndGet();
                    }
                }
                eventArrived.set(true);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalEventsSent; noOfEvents++) {
            inputHandler.send(new Object[]{(noOfEvents % noOfUniqueEvents) + ""});
        }

        SiddhiTestHelper.waitForEvents(200, totalEventsSent, totalEventsArrived, 60000);
        Assert.assertEquals(totalEventsSent, totalEventsArrived.get());
        Assert.assertTrue(eventArrived.get());
        Assert.assertTrue((double) validEvents.get() / totalEventsArrived.get() >= confidence);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"testApproximateCount_11"})
    public void testApproximateCount_12() throws InterruptedException {
        final int windowLength = 1000;
        final double confidence = 0.99;
        final double relativeError = 0.01;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "default relative error(" + relativeError + ") and confidence(" + confidence + ") - float input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number float);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalEventsArrived.incrementAndGet();

                    if (totalEventsArrived.get() < windowLength) {
                        exactCount.set((totalEventsArrived.get() / noOfUniqueEvents) + 1);
                    } else {
                        exactCount.set(windowLength / noOfUniqueEvents);
                    }

                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);

                    if (exactCount.get() >= lowerBound && exactCount.get() <= upperBound) {
                        validEvents.incrementAndGet();
                    }
                }
                eventArrived.set(true);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalEventsSent; noOfEvents++) {
            inputHandler.send(new Object[]{(float) ((noOfEvents % noOfUniqueEvents) + 0.001)});
        }
        SiddhiTestHelper.waitForEvents(200, totalEventsSent, totalEventsArrived, 60000);
        Assert.assertTrue(eventArrived.get());
        Assert.assertEquals(totalEventsSent, totalEventsArrived.get());
        Assert.assertTrue((double) validEvents.get() / totalEventsArrived.get() >= confidence);

        siddhiAppRuntime.shutdown();
    }
}

