# Cassandra Datadog

[![Circle CI](https://circleci.com/gh/SmartThingsOSS/cassandra-datadog.svg?style=svg)](https://circleci.com/gh/SmartThingsOSS/cassandra-datadog)

This library will be used with our custom reporter config library to enable us to export metrics from
cassandra directly to datadog without using jmx. Once the jar is compiled it will be placed into the cassandra
classpath and will be run within cassandra.  This allows us to tweak how we export things heavily, and control
what is sent. The downside is we must limit the dependencies we use to those that are used within cassandra.

This library started as a fork of https://github.com/coursera/metrics-datadog/tree/2.x-maintenance (Simplified BSD
License) since that library did basically what we needed to do here. We then tweaked things to work with the
dependencies as described above, and to also work with our custom reporter config library.
