package smartthings.cassandra.datadog;

import com.yammer.metrics.core.MetricName;

public interface MetricNameFormatter {

	public String format(MetricName name, String... path);
}
