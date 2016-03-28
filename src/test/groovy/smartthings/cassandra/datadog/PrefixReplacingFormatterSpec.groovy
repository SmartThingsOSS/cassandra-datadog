package smartthings.cassandra.datadog

import com.yammer.metrics.core.MetricName
import spock.lang.Specification

class PrefixReplacingFormatterSpec extends Specification {
	def 'truncate names as expected'() {
		given:
		PrefixReplacingFormatter formatter = new PrefixReplacingFormatter('org.apache.cassandra.metrics', 'cassandra')

		MetricName metricName = new MetricName("org.apache.cassandra.metrics", "keyspace", "EstimatedRowSizeHistogram", "myTable", "superlongString");

		when:
		String actual = formatter.format(metricName, DatadogReporter.Expansions.P95.toString())

		then:
		actual == "cassandra.keyspace.myTable.EstimatedRowSizeHistogram.p95"
	}

	def 'dont truncate if no match'() {
		given:
		PrefixReplacingFormatter formatter = new PrefixReplacingFormatter('org.apache.cassandra.metrics', 'cassandra')

		MetricName metricName = new MetricName("org.apache.bananas.metrics", "keyspace", "EstimatedRowSizeHistogram", "myTable", "superlongString")

		when:
		String actual = formatter.format(metricName, DatadogReporter.Expansions.P95.toString())

		then:
		actual == "org.apache.bananas.metrics.keyspace.myTable.EstimatedRowSizeHistogram.p95"
	}

}
