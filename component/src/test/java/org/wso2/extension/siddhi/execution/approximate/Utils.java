package org.wso2.extension.siddhi.execution.approximate;

import org.awaitility.Awaitility;
import org.awaitility.Duration;

public class Utils {
    public static void waitForVariableCount(int actual, int expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> actual == expected);
    }
}
