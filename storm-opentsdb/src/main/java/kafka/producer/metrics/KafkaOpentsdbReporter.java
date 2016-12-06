package kafka.producer.metrics;


import com.github.sps.metrics.OpenTsdbReporter;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincenty on 7/7/16.
 */
public class KafkaOpentsdbReporter extends ProducerReporter {
  public static final String OPENTSDB_URL = "opentsdb.url";

  String opentsdbUrl;

  OpenTsdbReporter reporter;
  public void init(List<KafkaMetric> metrics) {
    super.init(metrics);

    OpenTsdb opentsdb = OpenTsdb.forService(opentsdbUrl)
      .withGzipEnabled(true) // optional: compress requests to tsd
      .create();

    reporter = OpenTsdbReporter.forRegistry(registry)
      .prefixedWith(prefix)
      .withTags(tags)
      .build(opentsdb);
    reporter.start(reportInterval, TimeUnit.SECONDS);


  }

  public void close() {
    reporter.close();
  }

  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    opentsdbUrl = (String) configs.get(OPENTSDB_URL);
    Objects.requireNonNull(opentsdbUrl, "specify property opentsdb.url");
    Object reportIntervalObj = configs.get("reporter.opentsdb.interval");

  }

}
