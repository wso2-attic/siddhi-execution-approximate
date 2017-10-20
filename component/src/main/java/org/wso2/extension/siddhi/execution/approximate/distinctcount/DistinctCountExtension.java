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

package org.wso2.extension.siddhi.execution.approximate.distinctcount;

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
 * Performs HyperLogLog algorithm to get the approximate distinct count of events in a window.
 */
@Extension(
        name = "distinctCount",
        namespace = "approximate",
        description = "This applies the HyperLogLog algorithm to a Siddhi window. The algorithm is set with a " +
                "relative error and a confidence value to calculate the number of distinct events with an accepted " +
                "level of accuracy. Note that if this extension is used without a window, it may cause out of memory" +
                " errors. If you need to perform these calculations without windows, use the " +
                "`approximate:distinctCountEver` extension.",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value based on which the `distinctCount` is calculated.",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error to be allowed for the distinct count generated, " +
                                "expressed as a value between 0 and 1. Lower the value specified, lower is the " +
                                "rate by which the distinct count can deviate from being perfectly correct. If 0.01" +
                                " is specified, the distinct count generated must be almost perfectly accurate. If " +
                                "0.99 is specified, the minimal level of accuracy is expected. Note that you cannot " +
                                "specify `1` or `0` as the value for this parameter.",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description =  "This is the level confidence with which the specified relative error can " +
                                "be considered, specified as a rate. Higher the value specified, higher is the " +
                                "possibility of the amount of error in the distinct count being no greater than " +
                                "the relative error specified. If 0.99 is specified, it can be almost considered " +
                                "with certainty that the distinct count is generated with the specified rate of " +
                                "relative error. If 0.01 is specified, there can be minimal certainty as to whether" +
                                " the distinct count is generated with the specified rate of error. The possible " +
                                "values are `0.65`, `0.95`, `0.99`.",
                        type = {DataType.DOUBLE, DataType.FLOAT},
                        optional = true,
                        defaultValue = "0.95"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "distinctCount",
                        description = "This represents the distinct count based on the last event.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountLowerBound",
                        description = "The lowest value in the range within which the most accurate distinct count " +
                                "for the attribute is included This distinct count range is based on the latest event.",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountUpperBound",
                        description = "The highest value in the range within which the most accurate distinct count " +
                                "for the attribute is included This distinct count range is based on the latest event.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream requestStream (ip string);\n" +
                                "from requestStream#window.time(1000)#approximate:distinctCount(ip)\n" +
                                "select distinctCount, distinctCountLowerBound, distinctCountUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "This query calculates the distinct count of events for each IP address that " +
                                "has sent requests within the last 1000 milliseconds. The distinct count is 95% " +
                                "guaranteed to deviate no more than 1% from the actual distinct count per IP address." +
                                "The output consists of the approximate distinct count, and the lower bound and " +
                                "upper bound of the approximate answer."
                ),
                @Example(
                        syntax = "define stream sensorStream (sensorId int);\n" +
                                "from sensorStream#window.length(1000)\n" +
                                "#approximate:distinctCount(sensorId, 0.05, 0.65)\n" +
                                "select distinctCount, distinctCountLowerBound, distinctCountUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "This query calculates the distinct count of events for each sensor that has " +
                                "sent data to the stream. This value is calculated based on the last 1000 events " +
                                "in a sliding manner. The calculated distinct count is 65% guaranteed to deviate no " +
                                "more than 5% from the actual distinct count. The output consists of the " +
                                "approximate distinct count, and the lower bound and upper bound of the approximate" +
                                " answer."
                )
        }
)
public class DistinctCountExtension extends StreamProcessor {
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
                    attributeExpressionExecutors.length + " attributes are found inside the distinctCount function");
        }

        //expressionExecutors[0] --> value
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("The 1st parameter inside distinctCount function - " +
                    "'value' has to be a variable but found " +
                    this.attributeExpressionExecutors[0].getClass().getCanonicalName());
        }

        valueExecutor = attributeExpressionExecutors[0];

        //expressionExecutors[1] --> relativeError
        if (attributeExpressionExecutors.length > 1) {

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCount function " +
                        "- 'relative.error' has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[1].getReturnType() == Attribute.Type.FLOAT) {
                relativeError = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCount function - " +
                        "'relative.error' should be of type Double or Float but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if ((relativeError <= 0) || (relativeError >= 1)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCount function" +
                        " - 'relative.error' must be in the range of (0, 1) but found " + relativeError);
            }
        }

        //expressionExecutors[2] --> confidence
        if (attributeExpressionExecutors.length > 2) {
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCount function - " +
                        "'confidence' has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE ||
                    attributeExpressionExecutors[2].getReturnType() == Attribute.Type.FLOAT) {
                confidence = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCount function - " +
                        "'confidence' should be of type Double or Float but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            if (Math.abs(confidence - 0.65) > 0.0000001 && Math.abs(confidence - 0.95) > 0.0000001
                    && Math.abs(confidence - 0.99) > 0.0000001) {
                throw new SiddhiAppCreationException("The 3rd parameter inside distinctCount function - " +
                        "'confidence' must be a value from 0.65, 0.95 and 0.99 but found " + confidence);
            }
        }

        hyperLogLog = new HyperLogLog<>(relativeError, confidence, true);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("distinctCount", Attribute.Type.LONG));
        attributeList.add(new Attribute("distinctCountLowerBound", Attribute.Type.LONG));
        attributeList.add(new Attribute("distinctCountUpperBound", Attribute.Type.LONG));
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
                    } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                        hyperLogLog.removeItem(newData);
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
