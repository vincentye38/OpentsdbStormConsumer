package kafka.producer.metrics;


import com.github.sps.metrics.OpenTsdbReporter;
import com.github.sps.metrics.TaggedGauge;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.github.sps.metrics.opentsdb.OpenTsdb;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincenty on 7/7/16.
 */
public class KafkaOpentsdbReporter implements MetricsReporter {
  static String  uuid = UUID.randomUUID().toString();
  String opentsdbUrl;
  int reportInterval;
  Map<String, String> tags;

  TaggedMetricRegistry registry = new TaggedMetricRegistry();
  OpenTsdbReporter reporter;
  public void init(List<KafkaMetric> metrics) {
    OpenTsdb opentsdb = OpenTsdb.forService(opentsdbUrl)
      .withGzipEnabled(true) // optional: compress requests to tsd
      .create();
    Map<String, String> reporterTags = new HashMap<>();
    if (tags != null ) reporterTags.putAll(tags);
    reporterTags.put("reporterUUID", uuid);

    reporter = OpenTsdbReporter.forRegistry(registry)
      .prefixedWith("kafka.producer")
      .withTags(reporterTags)
      .build(opentsdb);
    reporter.start(reportInterval, TimeUnit.SECONDS);

    for (final KafkaMetric metric: metrics){
      registry.getOrRegisterTaggedMetric(getName(metric.metricName()), new DelegateMetric(metric));
    }
  }

  public void metricChange(final KafkaMetric metric) {
    String metricName = getName(metric.metricName());
    DelegateMetric delegateMetric = (DelegateMetric) registry.getOrRegisterTaggedMetric(metricName, new DelegateMetric(metric));
    delegateMetric.setMetric(metric);
  }

  public void metricRemoval(final KafkaMetric metric) {
    String metricName = getName(metric.metricName());
    String taggedName = TaggedMetricRegistry.getTaggedName(metricName, metric.metricName().tags());
    registry.remove(taggedName);
  }

  public void close() {
    reporter.close();
  }

  public void configure(Map<String, ?> configs) {
    opentsdbUrl = (String) configs.get("opentsdb.url");
    Objects.requireNonNull(opentsdbUrl, "specify property opentsdb.url");
    Object reportIntervalObj = configs.get("reporter.opentsdb.interval");
    tags = (Map<String, String>) configs.get("opentsdbReporter.tags");
    if (tags == null) tags = new HashMap<>();

    if (reportIntervalObj == null){
      reportInterval = 60;
    } else {
      if (reportIntervalObj instanceof String){
        reportInterval = Integer.parseInt((String)reportIntervalObj);
      } else {
        reportInterval = (Integer) reportIntervalObj;
      }
    }
  }

  String getName(MetricName metricName){
    return metricName.group() == null ?  metricName.name(): metricName.group() + "." + metricName.name();
  }

  static class DelegateMetric implements TaggedGauge<Double>{
    KafkaMetric metric;

    public DelegateMetric(KafkaMetric metric){
      this.metric = metric;
    }

    public Map<String, String> getTags() {
      return metric.metricName().tags();
    }

    public Double getValue() {
      return metric.value();
    }

    public void setMetric(KafkaMetric metric) {
      this.metric = metric;
    }
  }

}
