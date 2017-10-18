## Purpose
> If the user needs to calculate the distinct count and count of events in Siddhi streams
quickly with less memory utilization with agreed upon accuracy levels(less than 100%)
compared to the applicable naive approaches(in which, the accuracy is 100%),
the approximate computing approaches can be used.

## Goals
> Two approximate computing algorithms are implemented as Siddhi extensions which
allow the users to specify the accuracy levels and calculate distinct counts and counts of events in Siddhi streams.

## Approach
> N/A

## User stories
* Approximate Distinct Count Stream Extension
    * Used to find the approximate distinct count of events in a window of a Siddhi event stream specifying the relative error and confidence which control the accuracy to be maintained.
* Approximate Distinct Count Ever Stream Extension
    * Used to find the approximate distinct count of events in a Siddhi event stream specifying the relative error and confidence which control the accuracy to be maintained.
* Approximate Count Stream Extension
    * Used to find the approximate count(frequency) of events in a window of a Siddhi event stream based on a specific attribute specifying the relative error and confidence which control the accuracy to be maintained.

## Release note
> Approximate computing is a way of obtaining approximate results for specific calculations with significant savings of
 system resources and time. The siddhi-execution-approximate extension is an extension to Siddhi that is used to
 incorporate approximate computing functionalities to Siddhi queries. This release of
 siddhi-execution-approximate extension includes two approximation algorithms for
 approximate count of distinct elements and for approximate count of events based on a specific attribute.

## Documentation
> N/A

## Training
> N/A

## Certification
> N/A

## Marketing
> N/A

## Automation tests
 - Unit tests
   > Code coverage = 83%
 - Integration tests
   > N/A

## Security checks
 - Followed secure coding standards in http://wso2.com/technical-reports/wso2-secure-engineering-guidelines? yes
 - Ran FindSecurityBugs plugin and verified report? yes
 - Confirmed that this PR doesn't commit any keys, passwords, tokens, usernames, or other secrets? yes

## Samples
* Calculate the approximate distinct number of IP addresses which has sent requests within the last 1 min.
* Calculate the approximate distinct number of sensors which has sent data to a stream up to now.
* Calculate the approximate number of transactions done by different users out of all the last 1000 transactions.

## Related PRs
> List any other related PRs
> N/A

## Migrations (if applicable)
> N/A

## Test environment
> JDK version : 1.8.0_131, operating system : ubuntu 16.04
 
## Learning
> N/A