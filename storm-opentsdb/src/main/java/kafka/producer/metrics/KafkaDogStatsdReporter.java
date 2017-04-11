package kafka.producer.metrics;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.trulia.metrics.DogStatsDReporter;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincenty on 11/22/16.
 */
public class KafkaDogStatsdReporter extends ProducerReporter{
  public static final String STATSD_HOST = "statsD.host";
  public static final String STATSD_PORT = "statsD.port";
  DogStatsDReporter reporter;
  String hostName = "localhost";
  int port = 8125;


  @Override
  public void close() {
    if (reporter != null) reporter.close();
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    super.init(metrics);

    StatsDClient statsDClient = new NonBlockingStatsDClient(prefix, hostName, port);

    reporter = DogStatsDReporter.forRegistry(registry)
      .withTags(tags)
      .build(statsDClient);

    reporter.start(reportInterval, TimeUnit.SECONDS);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    Object portObj = configs.get(STATSD_PORT);
    if (portObj != null){
      Integer port = portObj instanceof Number ? ((Number) portObj).intValue() : Integer.parseInt((String) portObj) ;
      this.port = port;
    }
    String host = (String)configs.get(STATSD_HOST);
    if (host != null) {
      this.hostName = host;
    }



  }
}
