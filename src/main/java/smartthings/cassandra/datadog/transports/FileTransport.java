package smartthings.cassandra.datadog.transports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.cassandra.datadog.Transport;
import smartthings.cassandra.datadog.model.DatadogCounter;
import smartthings.cassandra.datadog.model.DatadogGauge;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class FileTransport implements Transport {
	private static final Logger LOG = LoggerFactory.getLogger(FileTransport.class);
	private final String fileName;
	private FileWriter fw;

	public FileTransport(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public Request prepare() throws IOException {
		fw = new FileWriter(fileName, true);
		fw.append("+++++++++++++++++++++ " + new Date().toString() + " +++++++++++++++++++++\n");
		return new FileRequest(this);
	}

	@Override
	public void close() throws IOException {
		if (fw != null) {
			fw.close();
		}
	}

	static class FileRequest implements Transport.Request {
		private final FileTransport transport;

		FileRequest(FileTransport transport) {
			this.transport = transport;
		}

		@Override
		public void addGauge(DatadogGauge gauge) throws IOException {
			Number epoch = gauge.getPoints().get(0).get(0);
			Number count = gauge.getPoints().get(0).get(1);
			transport.fw.append(String.format("%-10s %-10s %-12s %-20s %s\n", gauge.getHost(), gauge.getType(), epoch, count, gauge.getMetric()));
		}

		@Override
		public void addCounter(DatadogCounter counter) throws IOException {
			Number epoch = counter.getPoints().get(0).get(0);
			Number count = counter.getPoints().get(0).get(1);
			transport.fw.append(String.format("%-10s %-10s %-12s %-20s %s\n", counter.getHost(), counter.getType(), epoch, count, counter.getMetric()));
		}

		@Override
		public void send() throws Exception {
			transport.fw.flush();
			transport.fw.close();
			transport.fw = null;
		}
	}
}
