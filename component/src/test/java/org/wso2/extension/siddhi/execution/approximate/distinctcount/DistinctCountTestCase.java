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

package org.wso2.extension.siddhi.execution.approximate.distinctcount;


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

import java.util.concurrent.atomic.AtomicInteger;

public class DistinctCountTestCase {
    static final Logger LOG = Logger.getLogger(DistinctCountTestCase.class);
    private AtomicInteger totalCount;
    private int validCount;
    private final int totalNoOfEvents = 1000;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        totalCount = new AtomicInteger(0);
        validCount = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCardinality_1() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.95;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "default relative error(" + relativeError + ") and confidence(" + confidence + ") - int input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }
                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{noOfEvents});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_111() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.99;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "specified relative error(" + relativeError + ") and confidence(" + confidence + ")" +
                " - string input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }
                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{noOfEvents + ""});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_112() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.99;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "specified relative error(" + relativeError + ") and confidence(" + confidence + ")" +
                " - double input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }

                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{(double) noOfEvents});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_113() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.99;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "specified relative error(" + relativeError + ") and confidence(" + confidence + ")" +
                " - long input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }
                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{(long) noOfEvents});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_114() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.99;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "specified relative error(" + relativeError + ") and confidence(" + confidence + ")" +
                " - float input");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number float);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }

                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{(float) (noOfEvents + 0.001)});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_3() throws InterruptedException {
        final int windowLength = 500;
        final double relativeError = 0.05;
        final double confidence = 0.65;

        LOG.info("Approximate Distinct Count Test Case - for Siddhi length window - " +
                "specified relative error(" + relativeError + ") and confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")" +
                "#approximate:distinctCount(number, " + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    totalCount.incrementAndGet();
                    if (totalCount.get() < windowLength) {
                        exactCardinality = totalCount.get();
                    } else {
                        exactCardinality = windowLength;
                    }

                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (int noOfEvents = 0; noOfEvents < totalNoOfEvents; noOfEvents++) {
            inputHandler.send(new Object[]{noOfEvents});
        }
        SiddhiTestHelper.waitForEvents(200, totalNoOfEvents, totalCount, 60000);
        Assert.assertEquals(totalNoOfEvents, totalCount.get());
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount.get() >= confidence);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_4() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case - to check the number of parameters passed " +
                "to the distinctCount function are not 1 or 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, 0.1) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("1 or 3 attributes are expected but 2 attributes" +
                    " are found inside the distinctCount function"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_5() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case -  to validate the 2nd parameter inside distinctCount " +
                "function is a constant");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength +
                ")#approximate:distinctCount(number, number, 0.95) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside distinctCount function -" +
                    " 'relative.error' has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_6() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case - to validate the 2nd parameter inside distinctCount" +
                " function is a double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength
                + ")#approximate:distinctCount(number, '0.01', 0.65) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            exceptionOccurred = true;
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside distinctCount function" +
                    " - 'relative.error' should be of type Double or Float but found STRING"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_7() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case - to validate the 2nd parameter " +
                "inside distinctCount function is in (0, 1) range");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, 5.31, 0.99) " +
                "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 2nd parameter inside distinctCount function" +
                    " - 'relative.error' must be in the range of (0, 1) but found 5.31"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_8() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case -  to validate the 3rd parameter inside distinctCount" +
                " function is a constant");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, 0.01, number) "
                + "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside distinctCount function" +
                    " - 'confidence' has to be a constant but found " +
                    "org.wso2.siddhi.core.executor.VariableExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_9() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case - to validate the 3rd parameter inside distinctCount" +
                " function is a double");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, 0.01, '0.65') "
                + "select * " +
                "insert into outputStream;");

        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside distinctCount function" +
                    " - 'confidence' should be of type Double or Float but found STRING"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_10() throws InterruptedException {
        final int windowLength = 500;

        LOG.info("Approximate Distinct Count Test Case -  to validate the 3rd parameter " +
                "inside distinctCount function is a value out of 0.65, 0.95, 0.99");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(number, 0.01, 0.66) "
                + "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 3rd parameter inside distinctCount function - " +
                    "'confidence' must be a value from 0.65, 0.95 and 0.99 but found 0.66"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }

    @Test
    public void testApproximateCardinality_11() throws InterruptedException {
        final int windowLength = 1000;

        LOG.info("Approximate Distinct Count Test Case - to validate the 1st parameter " +
                "inside distinctCount function is a variable");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:distinctCount(12, 0.01, 0.66) "
                + "select * " +
                "insert into outputStream;");
        boolean exceptionOccurred = false;
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
        } catch (Exception e) {
            exceptionOccurred = true;
            Assert.assertTrue(e instanceof SiddhiAppCreationException);
            Assert.assertTrue(e.getCause().getMessage().contains("The 1st parameter inside distinctCount function - " +
                    "'value' has to be a variable but found" +
                    " org.wso2.siddhi.core.executor.ConstantExpressionExecutor"));
        }
        Assert.assertEquals(true, exceptionOccurred);
    }
}

