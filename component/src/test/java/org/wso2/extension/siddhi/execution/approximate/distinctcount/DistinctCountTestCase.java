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

public class DistinctCountTestCase {
    static final Logger LOG = Logger.getLogger(DistinctCountTestCase.class);
    private volatile int totalCount;
    private volatile int validCount;
    private final int noOfEvents = 1000;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        totalCount = 0;
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
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
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

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j});
            Thread.sleep(1);
        }
        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount >= confidence);
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
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
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

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j + ""});
            Thread.sleep(1);
        }
        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount >= confidence);
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
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
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

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{(double) j});
            Thread.sleep(1);
        }
        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount >= confidence);
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
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
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

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{(long) j});
            Thread.sleep(1);
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount >= confidence);
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
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
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

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j});
            Thread.sleep(1);
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);
//      confidence check
        Assert.assertTrue((double) validCount / totalCount >= confidence);
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

