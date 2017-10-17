## Purpose
> Naive approaches allowed in Siddhi to calculate distinct count and count of events in streams cost lots of memory and add high latency to do calculations.
> Those issues can be addressed by allowing the user to control the accuracy of answers and calculate results quickly with less memory utilization.

## Goals
> Two approximate computing algorithms are implemented as Siddhi Stream Processor extensions which allow the users to change the accuracy levels are calculate distinct counts and counts of events in Siddhi streams.

## Approach
> N/A

## User stories
> Approximate Distinct Count Stream Extension
>> Used to find the approximate distinct count of events in a window of a Siddhi event stream specifying the relative error and confidence to be maintained.
> Approximate Distinct Count Ever Stream Extension
>> Used to find the approximate distinct count of events in a Siddhi event stream specifying the relative error and confidence to be maintained.
> Approximate Count Stream Extension
>> Used to find the approximate count(frequency) of events in a window of a Siddhi event stream based on a specific attribute specifying the relative error and confidence to be maintained.

## Release note
> Brief description of the new feature or bug fix as it will appear in the release notes

## Documentation
> Link(s) to product documentation that addresses the changes of this PR. If no doc impact, enter “N/A” plus brief explanation of why there’s no doc impact


## Training
> N/A

## Certification
> N/A

## Marketing
> N/A

## Automation tests
 - Unit tests
   > Code coverage = 82%
 - Integration tests
   > N/A

## Security checks
 - Followed secure coding standards in http://wso2.com/technical-reports/wso2-secure-engineering-guidelines? yes
 - Ran FindSecurityBugs plugin and verified report? yes
 - Confirmed that this PR doesn't commit any keys, passwords, tokens, usernames, or other secrets? yes

## Samples
> Provide high-level details about the samples related to this feature

## Related PRs
> List any other related PRs
> N/A

## Migrations (if applicable)
> N/A

## Test environment
> JDK version : 1.8.0_131, operating system : ubuntu 16.04
 
## Learning
> N/A