# API Docs - v1.0.0-SNAPSHOT

## Approximate

### distinctCount *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">Performs HyperLogLog algorithm on a window of streaming data set based on a specific relative error and a confidence value to calculate the number of distinct events. If used without a window, the out of memory errors will occur. For usage without the window, use the approximate:distinctCountEver extension.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
approximate:distinctCount(<INT|DOUBLE|FLOAT|LONG|STRING|BOOL|TIME|OBJECT> value, <DOUBLE|FLOAT> relative.error, <DOUBLE|FLOAT> confidence)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">value</td>
        <td style="vertical-align: top; word-wrap: break-word">The value used to find distinctCount</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>DOUBLE<br>FLOAT<br>LONG<br>STRING<br>BOOL<br>TIME<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">relative.error</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the relative error for which the distinct count is obtained. The values must be in the range of (0, 1).</td>
        <td style="vertical-align: top">0.01</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">confidence</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the confidence for which the relative error is true. The value must be one out of 0.65, 0.95, 0.99.</td>
        <td style="vertical-align: top">0.95</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCount</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the distinct count considering the last event </td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCountLowerBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the lower bound of the distinct count considering the last event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCountUpperBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the upper bound of the distinct count considering the last event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream requestStream (ip string);
from requestStream#window.time(1000)#approximate:distinctCount(ip)
select distinctCount, distinctCountLowerBound, distinctCountUpperBound
insert into OutputStream;

```
<p style="word-wrap: break-word">Distinct count of ip addresses which has sent requests within the last 1000ms is calculated for a default relative error of 0.01 and a default confidence of 0.95. Here the distinct count is the number of different values received for ip attribute considering the events received within last 1000ms time period. The answers are 95% guaranteed to have a +-1% error relative to the distinct count. The output will consist of the approximate distinct count, lower bound and upper bound of the approximate answer.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream sensorStream (sensorId int);
from sensorStream#window.length(1000)
#approximate:distinctCount(sensorId, 0.05, 0.65)
select distinctCount, distinctCountLowerBound, distinctCountUpperBound
insert into OutputStream;

```
<p style="word-wrap: break-word">Distinct count of sensors which has sent data to the stream out of last 1000 events is calculated for a relative error of 0.05 and a confidence of 0.65. Here the distinct count is the number of different values values received for sensorId attribute in the last 1000 events. The answers are 65% guaranteed to have a +-5% error relative to the distinct count. The output will consist of the approximate distinct count, lower bound and upper bound of the approximate answer.</p>

### count *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">Performs Count-min-sketch algorithm on a window of streaming data set based on a specific relative error and  a confidence value to calculate the approximate count(frequency) of events. Using without a window may return out of memory errors.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
approximate:count(<INT|DOUBLE|FLOAT|LONG|STRING|BOOL|TIME|OBJECT> value, <DOUBLE|FLOAT> relative.error, <DOUBLE|FLOAT> confidence)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">value</td>
        <td style="vertical-align: top; word-wrap: break-word">The value used to find the count</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>DOUBLE<br>FLOAT<br>LONG<br>STRING<br>BOOL<br>TIME<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">relative.error</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the relative error for which the count is obtained. The values must be in the range of (0, 1).</td>
        <td style="vertical-align: top">0.01</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">confidence</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the confidence for which the relative error is true. The values must be in the range of (0, 1).</td>
        <td style="vertical-align: top">0.99</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">count</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the approximate count per attribute considering the latest event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">countLowerBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the lower bound of the count per attribute considering the latest event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">countUpperBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the upper bound of the count per attribute considering the latest event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream requestStream (ip string);
from requestStream#window.time(1000)#approximate:count(ip)
select count, countLowerBound, countUpperBound
insert into OutputStream;
```
<p style="word-wrap: break-word">Count(frequency) of requests from different ip addresses in a time window is calculated for a default relative error of 0.01 and a default confidence of 0.99. Here the counts are calculated considering only the events belong to the last 1000 ms. The answers are 99% guaranteed to have a +-1% error relative to the total event count within the window. The output will consist of the approximate count of the latest event, lower bound and upper bound of the approximate answer.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream transactionStream (userId int, amount double);
from transactionStream#window.length(1000)#approximate:count(userId, 0.05, 0.9)
select count, countLowerBound, countUpperBound
insert into OutputStream;
```
<p style="word-wrap: break-word">Count(frequency) of transactions done by different users out of last 1000 transactions based on the userId is calculated for an relative error of 0.05 and a confidence of 0.9. Here the counts are calculated considering only the last 1000 events arrived. The answers are 90% guaranteed to have a +-5%The answers are 99% guaranteed to have a +-5% error relative to the total event count within the window.The output will consist of the approximate count of the latest event, lower bound and upper bound of the approximate answer.</p>

### distinctCountEver *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>*

<p style="word-wrap: break-word">Performs HyperLogLog algorithm on a streaming data set based on a specific relative error and a confidence value to calculate the number of distinct events. If used with a window, errorneous results will be returned. For usage with the window, use the approximate:distinctCount extension.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
approximate:distinctCountEver(<INT|DOUBLE|FLOAT|LONG|STRING|BOOL|TIME|OBJECT> value, <DOUBLE|FLOAT> relative.error, <DOUBLE|FLOAT> confidence)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">value</td>
        <td style="vertical-align: top; word-wrap: break-word">The value used to find distinctCount</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>DOUBLE<br>FLOAT<br>LONG<br>STRING<br>BOOL<br>TIME<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">relative.error</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the relative error for which the distinct count is obtained. The values must be in the range of (0, 1).</td>
        <td style="vertical-align: top">0.01</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">confidence</td>
        <td style="vertical-align: top; word-wrap: break-word">This is the confidence for which the relative error is true. The value must be one out of 0.65, 0.95, 0.99.</td>
        <td style="vertical-align: top">0.95</td>
        <td style="vertical-align: top">DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCountEver</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the distinct count considering the last event </td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCountEverLowerBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the lower bound of the distinct count considering the last event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
    <tr>
        <td style="vertical-align: top">distinctCountEverUpperBound</td>
        <td style="vertical-align: top; word-wrap: break-word">Represents the upper bound of the distinct count considering the last event</td>
        <td style="vertical-align: top">LONG</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream requestStream (ip string);
from requestStream#approximate:distinctCountEver(ip)
select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound
insert into OutputStream;

```
<p style="word-wrap: break-word">Distinct count of ip addresses which has sent requests is calculated for a default relative error of 0.01 and a default confidence of 0.95. Here the distinct count is the number of different values received for ip attribute. The answers are 95% guaranteed to have a +-1% error relative to the distinct count. The output will consist of the approximate distinct count, lower bound and upper bound of the approximate answer.</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream sensorStream (sensorId int);
from sensorStream#approximate:distinctCountEver(sensorId, 0.05, 0.65)
select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound
insert into OutputStream;

```
<p style="word-wrap: break-word">Distinct count of sensors which has sent data to the stream is calculated for a relative error of 0.05 and a confidence of 0.65. Here the distinct count is the number of different values received for sensorId attribute. The answers are 65% guaranteed to have a +-5% error relative to the distinct count. The output will consist of the approximate distinct count, lower bound and upper bound of the approximate answer.</p>

