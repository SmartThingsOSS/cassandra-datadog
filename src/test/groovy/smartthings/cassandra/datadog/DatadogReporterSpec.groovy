package smartthings.cassandra.datadog

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Clock
import com.yammer.metrics.core.MetricPredicate
import com.yammer.metrics.core.MetricsRegistry
import smartthings.cassandra.datadog.transports.HttpTransport
import spock.lang.Specification

class DatadogReporterSpec extends Specification {
	def "test things are setup properly"() {
		given:
		String apiKey = "1234"
		Set<DatadogReporter.Expansions> exs = [DatadogReporter.Expansions.P95].toSet()
		MetricsRegistry registry = new MetricsRegistry()

		when:
		DatadogReporter reporter = new DatadogReporter.Builder()
			.withApiKey(apiKey)
			.withMetricsRegistry(Metrics.defaultRegistry())
			.withExpansions(EnumSet.copyOf(exs))
			.withPredicate(MetricPredicate.ALL)
			.withVmMetricsEnabled(false)
			.withMetricNameFormatter(new PrefixReplacingFormatter("Test", "Yo"))
			.withHost('test1')
			.build()

		then:
		reporter.predicate == MetricPredicate.ALL
		reporter.tags == []
		reporter.expansions == exs
		reporter.clock == Clock.defaultClock()
		reporter.transport.class == HttpTransport
		reporter.metricNameFormatter.class == PrefixReplacingFormatter
		((HttpTransport) reporter.transport).seriesUrl == new URL("https://app.datadoghq.com/api/v1/series?api_key=${apiKey}")
		reporter.host == 'test1'
		!reporter.printVmMetrics
	}
}
