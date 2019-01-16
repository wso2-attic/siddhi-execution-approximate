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
        description = "This extension applies the `count-min sketch` algorithm to a Siddhi window. The algorithm" +
                " calculates the approximate count i.e., the frequency of events that arrive, based on " +
                " the given values for the 'relative error' and 'confidence value'." +
                " Note that, using this extension without a window may cause an 'out of memory' error.",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value for which the count is derived.",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error to be allowed for the count generated, expressed " +
                                "as a value between 0 and 1. Lower the value specified, lower is the rate by which " +
                                "the count can deviate from being perfectly correct. If 0.01 is specified, the count " +
                                "generated must be almost perfectly accurate. If 0.99 is specified, minimal " +
                                "level of accuracy is expected. Note that you cannot specify `1` or `0` as the value " +
                                "for this parameter.",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "This value determines the rate by which the result can deviate from " +
                                "the actual event count. " +
                                "Higher the value specified, higher is the " +
                                "possibility of the amount of error in the count being no greater than the relative " +
                                "error specified. If 0.99 is specified, it is almost certain " +
                                "that the count is generated with the specified rate of relative error. If 0.01 is" +
                                " specified, there can be minimal certainty as to whether the count is generated " +
                                "with the specified rate of error. Note that you cannot specify `1` or `0` as the " +
                                "value for this parameter.",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.99"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "count",
                        description = "This represents the approximate count per attribute based on the latest " +
                                "event.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "countLowerBound",
                        description = "The lowest value in the range within which the most accurate count for the " +
                                "attribute is included. This count range is based on the latest event.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "countUpperBound",
                        description = "The highest value in the range within which the most accurate count for the " +
                                "attribute is included. This count range is based on the latest event.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream RequestStream (ip string);\n" +
                                "from RequestStream#window.time(1000)#approximate:count(ip)\n" +
                                "select count, countLowerBound, countUpperBound\n" +
                                "insert into OutputStream;",
                        description = "This query generates the count(frequency) of requests from different IP " +
                                "addresses in a time window that is calculated with a default relative error of 0.01 " +
                                "and a default confidence of 0.99. " +
                                "Here, only the events that arrive during the last 1000 milliseconds are considered" +
                                " when calculating the counts. The counts generated are 99% guaranteed to deviate " +
                                "from the actual count within the window by only 1%. The output consists of the " +
                                "approximate count of the latest event, lower bound, and upper bound of the " +
                                "approximate answer."
                ),
                @Example(
                        syntax = "define stream TransactionStream (userId int, amount double);\n" +
                                "from TransactionStream#window.length(1000)#approximate:count(userId, 0.05, 0.9)\n" +
                                "select count, countLowerBound, countUpperBound\n" +
                                "insert into OutputStream;",
                        description = "This query generates the count or frequency of transactions for each user ID " +
                                "based on the last 1000 transactions, i.e., events. The counts generated are 90% " +
                                "guaranteed to deviate from the actual event count within the window by only 5%." +
                                "The output consists of the approximate count of the latest events, " +
                                "lower bound and upper bound of the approximate result."
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
        final double defaultRelativeError = 0.01;
        final double defaultConfidence = 0.99;

        double relativeError = defaultRelativeError;
        double confidence = defaultConfidence;

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

                Object[] outputData = {approximateCount, confidenceInterval[0], confidenceInterval[1]};

                complexEventPopulater.populateComplexEvent(streamEvent, outputData);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() { }

    @Override
    public void stop() { }

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
