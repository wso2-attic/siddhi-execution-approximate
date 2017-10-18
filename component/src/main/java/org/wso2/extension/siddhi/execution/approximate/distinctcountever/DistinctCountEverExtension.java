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

package org.wso2.extension.siddhi.execution.approximate.distinctcountever;

import org.wso2.extension.siddhi.execution.approximate.distinctcount.HyperLogLog;
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
 * Performs HyperLogLog algorithm to get the approximate distinct count of events in a stream.
 */
@Extension(
        name = "distinctCountEver",
        namespace = "approximate",
        description = "Performs HyperLogLog algorithm on a streaming data set based on a specific relative error" +
                " and a confidence value to calculate the number of distinct events. " +
                "If used with a window, errorneous results will be returned. " +
                "For usage with the window, use the approximate:distinctCount extension.",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find distinctCount",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error for which the distinct count is obtained. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "This is the confidence for which the relative error is true. " +
                                "The value must be one out of 0.65, 0.95, 0.99.",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.95"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "distinctCountEver",
                        description = "Represents the distinct count considering the last event ",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountEverLowerBound",
                        description = "Represents the lower bound of the distinct count considering the last event",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountEverUpperBound",
                        description = "Represents the upper bound of the distinct count considering the last event",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream requestStream (ip string);\n" +
                                "from requestStream#approximate:distinctCountEver(ip)\n" +
                                "select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "Distinct count of ip addresses which has sent requests is calculated for" +
                                " a default relative error of 0.01 and a default confidence of 0.95. " +
                                "Here the distinct count is the number of different values received for " +
                                "ip attribute. The answers are 95% guaranteed to have a +-1% error " +
                                "relative to the distinct count. The output will consist of the approximate " +
                                "distinct count, lower bound and upper bound of the approximate answer."
                ),
                @Example(
                        syntax = "define stream sensorStream (sensorId int);\n" +
                                "from sensorStream#approximate:distinctCountEver(sensorId, 0.05, 0.65)\n" +
                                "select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "Distinct count of sensors which has sent data to the stream is calculated " +
                                "for a relative error of 0.05 and a confidence of 0.65. " +
                                "Here the distinct count is the number of different values received for " +
                                "sensorId attribute. The answers are 65% guaranteed to have a +-5% error " +
                                "relative to the distinct count. The output will consist of the approximate " +
                                "distinct count, lower bound and upper bound of the approximate answer."

                )
        }
)
public class DistinctCountEverExtension extends StreamProcessor {
    private HyperLogLog<Object> hyperLogLog;

    private ExpressionExecutor valueExecutor;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

//      default values for relative error and confidence
        final double defaultRelativeError = 0.01;
        final double defaultConfidence = 0.95;

        double relativeError = defaultRelativeError;
        double confidence = defaultConfidence;

//       validate number of attributes
        if (!(attributeExpressionExecutors.length == 1 || attributeExpressionExecutors.length == 3)) {
            throw new SiddhiAppCreationException("1 or 3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the " +
                    "distinctCountEver function");
        }

        //expressionExecutors[0] --> value
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("The 1st parameter inside distinctCountEver function - " +
                    "'value' has to be a variable but found " +
                    this.attributeExpressionExecutors[0].getClass().getCanonicalName());
        }
        valueExecutor = attributeExpressionExecutors[0];

        //expressionExecutors[1] --> relativeError
        if (attributeExpressionExecutors.length > 1) {

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function " +
                        "- 'relative.error' has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) {
                relativeError = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function - " +
                        "'relative.error' should be of type Double or Float but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if ((relativeError <= 0) || (relativeError >= 1)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function" +
                        " - 'relative.error' must be in the range of (0, 1) but found " + relativeError);
            }
        }

        //expressionExecutors[2] --> confidence
        if (attributeExpressionExecutors.length > 2) {
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCountEver function - " +
                        "'confidence' has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) {
                confidence = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCountEver function - " +
                        "'confidence' should be of type Double or Float but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            if (Math.abs(confidence - 0.65) > 0.0000001 && Math.abs(confidence - 0.95) > 0.0000001
                    && Math.abs(confidence - 0.99) > 0.0000001) {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCountEver function - " +
                        "'confidence' must be a value from 0.65, 0.95 and 0.99 but found " + confidence);
            }
        }

        hyperLogLog = new HyperLogLog<>(relativeError, confidence, false);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("distinctCountEver", Attribute.Type.LONG));
        attributeList.add(new Attribute("distinctCountEverLowerBound", Attribute.Type.LONG));
        attributeList.add(new Attribute("distinctCountEverUpperBound", Attribute.Type.LONG));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                Object newData = valueExecutor.execute(streamEvent);
                if (newData == null) {
                    streamEventChunk.remove();
                } else {
                    if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                        hyperLogLog.addItem(newData);
                    } else if (streamEvent.getType().equals(StreamEvent.Type.RESET)) {
                        hyperLogLog.clear();
                    }

                    Object[] outputData = {hyperLogLog.getCardinality(), hyperLogLog.getConfidenceInterval()[0],
                            hyperLogLog.getConfidenceInterval()[1]};

                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public Map<String, Object> currentState() {
        synchronized (this) {
            Map<String, Object> map = new HashMap();
            map.put("hyperLogLog", hyperLogLog);
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            hyperLogLog = (HyperLogLog) map.get("hyperLogLog");
        }
    }
}
