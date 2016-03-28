package smartthings.cassandra.datadog.transports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.datadog.Transport;
import smartthings.cassandra.datadog.model.DatadogCounter;
import smartthings.cassandra.datadog.model.DatadogGauge;
import smartthings.cassandra.datadog.serializer.JsonSerializer;
import smartthings.cassandra.datadog.serializer.Serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Uses the datadog http webservice to push metrics.
 *
 * @see <a href="http://docs.datadoghq.com/api/">API docs</a>
 */
public class HttpTransport implements Transport {
	private static final Logger LOG = LoggerFactory.getLogger(HttpTransport.class);
	private static final int GATEWAY_TIMEOUT_MILLIS = 5000;

	private final URL seriesUrl;

	public HttpTransport(String apiKey) {
		try {
			this.seriesUrl = new URL(String.format("https://app.datadoghq.com/api/v1/series?api_key=%s", apiKey));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Unable to form URL", e);
		}
	}

	@Override
	public Request prepare() throws IOException {
		return new HttpRequest(this);
	}

	@Override
	public void close() throws IOException {
		// nothing to close
	}

	public URL getSeriesUrl() {
		return seriesUrl;
	}

	static class HttpRequest implements Transport.Request {
		private final Serializer serializer;
		private final HttpTransport transport;

		HttpRequest(HttpTransport transport) throws IOException {
			this.transport = transport;
			serializer = new JsonSerializer();
			serializer.startObject();
		}

		@Override
		public void addGauge(DatadogGauge gauge) throws IOException {
			serializer.appendGauge(gauge);
		}

		@Override
		public void addCounter(DatadogCounter counter) throws IOException {
			serializer.appendCounter(counter);
		}

		@Override
		public void send() throws Exception {
			serializer.endObject();
			postMetric(serializer.getAsString());
		}

		private void postMetric(final String messageJson) {
			HttpURLConnection urlConnection = null;
			try {
				LOG.debug("sending data to the datadog gateway");
				urlConnection = (HttpURLConnection) transport.seriesUrl.openConnection();

				urlConnection.setRequestMethod("POST");
				urlConnection.setDoInput(true);
				urlConnection.setDoOutput(true);
				urlConnection.setUseCaches(false);
				urlConnection.setConnectTimeout(GATEWAY_TIMEOUT_MILLIS);
				urlConnection.setReadTimeout(GATEWAY_TIMEOUT_MILLIS);
				urlConnection.setRequestProperty("content-type", "application/json; charset=utf-8");

				OutputStream os = urlConnection.getOutputStream();
				os.write(messageJson.getBytes());
				os.flush();
				os.close();

				int responseCode = urlConnection.getResponseCode();
				if (responseCode >= 300) {
					LOG.warn("Datadog returned a non-200 response: {}", responseCode);
				}
			} catch (Exception e) {
				LOG.error("Error connecting to datadog", e);
			} finally {
				if (urlConnection != null) {
					urlConnection.disconnect();
				}
			}
		}
	}
}
