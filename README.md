# DataEngineering

This is an interview challenge for Peerislands. Please feel free to fork. Pull Requests will be ignored.


## Processing & Analytical goals:
Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)

Determine the average session time

Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

Find the most engaged users, ie the IPs with the longest session times


## Tools allowed :
Spark (any language, but prefer Scala)

Databricks [Can use Databricks community edition and commit your notebook]

### Additional notes:
You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.

## How to complete this challenge:
Fork this repo in github
Complete the processing and analytics as defined first to the best of your ability with the time provided.
Place notes in your code to help with clarity where appropriate. 
Include the test code and data in your solution.
