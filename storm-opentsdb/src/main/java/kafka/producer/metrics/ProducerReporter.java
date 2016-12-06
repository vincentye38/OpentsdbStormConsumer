package kafka.producer.metrics;

import com.github.sps.metrics.TaggedGauge;
import com.github.sps.metrics.TaggedMetricRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by vincenty on 11/22/16.
 */
public abstract class ProducerReporter implements MetricsReporter {
  public static final String PREFIX = "reporter.prefix";
  public static final String REPORTER_INTERVAL = "reporter.interval";
  public static final String REPORTER_TAGS = "reporter.tags";


  //TODO why is static?
  static String  uuid = UUID.randomUUID().toString();
  int reportInterval = 60;
  Map<String, String> tags = new HashMap<>();
  TaggedMetricRegistry registry = new TaggedMetricRegistry();
  String prefix = "kafka.producer";

  public void init(List<KafkaMetric> metrics) {
    tags.put("reporterUUID", uuid);
    for (final KafkaMetric metric: metrics){
      registry.getOrRegisterTaggedMetric(getName(metric.metricName()), new DelegateMetric(metric));
    }
  }

  public void configure(Map<String, ?> configs) {
    String prefix = (String) configs.get(PREFIX);
    if (prefix != null && !prefix.isEmpty()) this.prefix = prefix;

    Object reportIntervalObj = configs.get(REPORTER_INTERVAL);
    if (reportIntervalObj != null){
      if (reportIntervalObj instanceof String){
        reportInterval = Integer.parseInt((String)reportIntervalObj);
      } else {
        reportInterval = (Integer) reportIntervalObj;
      }
    }

    Map<String, String> tags = (Map<String, String>) configs.get(REPORTER_TAGS);
    if (tags != null) this.tags.putAll(tags);
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

  String getName(MetricName metricName){
    return metricName.group() == null ?  metricName.name(): metricName.group() + "." + metricName.name();
  }

  static class DelegateMetric implements TaggedGauge<Double> {
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
