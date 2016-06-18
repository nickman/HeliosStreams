# HeliosStreams
Streaming performance data all the way home.

## Data Types

## Data Format Specifications

#### Value Types

 Symbol | Name | Type | Description | Parameters
-------- | -------- | -------- | -------- | --------
 **AC** | Accumulator | Valueless | Tracking an all time total of a specific event type. | None
 **M** | Meter | Valueless | Tracking the per seconds/minutes/hours/day rate of a specific event type | None
 **D** | Delta | Value | The value of a given metric instance is the delta of the current and the prior value | **TBD**
 **AG** | Period Aggregated | Value | Values are aggregated to count, min, max and average per defined period (e.g. 15 sedonds)  | **TBD**
 **S** | Straight Through | Value | The metric is not aggregated and forwarded directly to endpoint | None


# Plain Text

The plain text stream is typically used when extracting data from application log or some other unstructured data feed after performing an extract-and-format operation in an intermediary such as [LogStash](https://www.elastic.co/products/logstash) and/or [Filebeat](https://www.elastic.co/products/beats/filebeat).

#### Data Type Routing
- **Directed**: The line of text is beig submitted to a stream that is specific to the data type of the metric.
- **Undirected**: The line of text  contains a symbol representing the data type of the metric and will be routed to an appropriate stream processor according to that symbol.

#### Text Line Format

*\[&lt;value-type&gt;,\]&lt;timestamp&gt;, \[&lt;value&gt;,\] &lt;metric-name&gt;, &lt;host&gt;, &lt;app&gt; \[,&lt;tagkey1&gt;=&lt;tagvalue1&gt;,&lt;tagkey*n*&gt;=&lt;tagvalue*n*&gt;\]*

- **value-type**: The value type symbol indicating how the metric should be routed. Only required if the routing is undirected.
- **timestamp**: The effective timestamp of the metric in seconds-since-the epoch or milliseconds-since-the epoch.
- **value**: The metric numeric value. Absent in valueless types.
- **metric-name**: The identifier of the meaning of the metric. The fully qualified (global) name is comprised of the **metric-name**, the **host**, the **app** and the **tags**.
- **host** : The name of the host the metric originated from
- **app** : The name of the application the metric originated from
- **tags** : Key value pairs that qualify the instance of the **metric-name**.






