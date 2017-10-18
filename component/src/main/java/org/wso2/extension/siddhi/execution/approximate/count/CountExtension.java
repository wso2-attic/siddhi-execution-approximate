/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.approximate.count;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs Count-Min Sketch algorithm to get the approximate count(frequency) of events in a window.
 */
@Extension(
        name = "count",
        namespace = "approximate",
        description = "Performs Count-min-sketch algorithm on a window of streaming data set based on a specific " +
                "relative error and  a confidence value to calculate the approximate count(frequency) of events. " +
                "Using without a window may return out of memory errors.",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find the count",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error for which the count is obtained. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "This is the confidence for which the relative error is true. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.99"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "count",
                        description = "Represents the approximate count per attribute considering the latest event",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "countLowerBound",
                        description = "Represents the lower bound of the count per attribute" +
                                " considering the latest event",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "countUpperBound",
                        description = "Represents the upper bound of the count per attribute " +
                                "considering the latest event",
                        type = {DataType.LONG}
                )
        },
        examples = {

                @Example(
                        syntax = "define stream requestStream (ip string);\n" +
                                "from requestStream#window.time(1000)#approximate:count(ip)\n" +
                                "select count, countLowerBound, countUpperBound\n" +
                                "insert into OutputStream;",
                        description = "Count(frequency) of requests from different ip addresses" +
                                " in a time window is calculated for a default relative error of 0.01 " +
                                "and a default confidence of 0.99. " +
                                "Here the counts are calculated considering only the events belong" +
                                " to the last 1000 ms. The answers are 99% guaranteed to have a +-1% error " +
                                "relative to the total event count within the window. " +
                                "The output will consist of the " +
                                "approximate count of the latest event, lower bound and " +
                                "upper bound of the approximate answer."
                ),
                @Example(
                        syntax = "define stream transactionStream (userId int, amount double);\n" +
                                "from transactionStream#window.length(1000)#approximate:count(userId, 0.05, 0.9)\n" +
                                "select count, countLowerBound, countUpperBound\n" +
                                "insert into OutputStream;",
                        description = "Count(frequency) of transactions done by different users out of " +
                                "last 1000 transactions based on the userId is " +
                                "calculated for an relative error of 0.05 and a confidence of 0.9. " +
                                "Here the counts are calculated considering only the last 1000 events arrived. " +
                                "The answers are 90% guaranteed to have a +-5%The answers are 99% guaranteed to " +
                                "have a +-5% error relative to the total event count within the window." +
                                "The output will consist of the approximate count of the latest event, " +
                                "lower bound and upper bound of the approximate answer."
                )
        }
)
public class CountExtension extends StreamProcessor {

    private CountMinSketch<Object> countMinSketch;

    private ExpressionExecutor valueExecutor;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

//      default values for relative error and confidence
        double relativeError = 0.01;
        double confidence = 0.99;

//       validate number of attributes
        if (!(attributeExpressionExecutors.length == 1 || attributeExpressionExecutors.length == 3)) {
            throw new SiddhiAppCreationException("1 or 3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the count function");
        }

        //expressionExecutors[0] --> value
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("The 1st parameter inside count function - " +
                    "'value' has to be a variable but found " +
                    this.attributeExpressionExecutors[0].getClass().getCanonicalName());
        }
        valueExecutor = attributeExpressionExecutors[0];


        //expressionExecutors[1] --> relativeError
        if (attributeExpressionExecutors.length > 1) {
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside count function - " +
                        "'relative.error' has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) {
                relativeError = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 2nd parameter inside count function - " +
                        "'relative.error' should be of type Double or Float but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if ((relativeError <= 0) || (relativeError >= 1)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside count function - " +
                        "'relative.error' must be in the range of (0, 1) but found " + relativeError);
            }
        }

        //expressionExecutors[2] --> confidence
        if (attributeExpressionExecutors.length > 2) {

            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 3rd parameter inside count function - " +
                        "'confidence' has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) {
                confidence = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 3rd parameter inside count function - " +
                        "'confidence' should be of type Double or Float but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            if ((confidence <= 0) || (confidence >= 1)) {
                throw new SiddhiAppCreationException("The 3rd parameter inside count function - " +
                        "'confidence' must be in the range of (0, 1) but found " + confidence);
            }
        }

        countMinSketch = new CountMinSketch<>(relativeError, confidence);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("count", Attribute.Type.LONG));
        attributeList.add(new Attribute("countLowerBound", Attribute.Type.LONG));
        attributeList.add(new Attribute("countUpperBound", Attribute.Type.LONG));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        long approximateCount = 0;
        long[] confidenceInterval = new long[2];

        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            Object newData = valueExecutor.execute(streamEvent);
            if (newData == null) {
                streamEventChunk.remove();
            } else {
                if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    approximateCount = countMinSketch.insert(newData);
                    confidenceInterval = countMinSketch.getConfidenceInterval(approximateCount);
                } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    approximateCount = countMinSketch.remove(newData);
                    confidenceInterval = countMinSketch.getConfidenceInterval(approximateCount);
                } else if (streamEvent.getType().equals(StreamEvent.Type.RESET)) {
                    countMinSketch.clear();
                }
//              outputData = {count, lower bound, upper bound}
                Object[] outputData = {approximateCount, confidenceInterval[0], confidenceInterval[1]};

                complexEventPopulater.populateComplexEvent(streamEvent, outputData);
            }
        }

        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        synchronized (this) {
            Map<String, Object> map = new HashMap();
            map.put("countMinSketch", countMinSketch);
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            countMinSketch = (CountMinSketch) map.get("countMinSketch");
        }
    }
}
