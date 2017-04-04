package com.trulia.metrics;

import com.codahale.metrics.*;
import com.github.sps.metrics.TaggedCounter;
import com.github.sps.metrics.TaggedMeter;
import com.github.sps.metrics.TaggedMetric;
import com.github.sps.metrics.TaggedMetricRegistry;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincenty on 11/28/16.
 */
public class DogStatsDReporter extends ScheduledReporter{
  StatsDClient statsDClient;
  Map<String, String> tags;
  Logger logger = LoggerFactory.getLogger(this.getClass());

  public DogStatsDReporter(MetricRegistry registry,  MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, StatsDClient statsDClient, Map<String, String> tags, String prefix) {
    super(registry, "DogStatsD-reporter", filter, rateUnit, durationUnit);
    this.statsDClient =   statsDClient;
    this.tags = tags == null ? new HashMap<>() : tags;
  }
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    gauges.forEach((name, value) -> reportGauge(TaggedMetricRegistry.getBaseName(name), value));
    counters.forEach((name, value) -> reportCounter(TaggedMetricRegistry.getBaseName(name), value));
    histograms.forEach((name, value) -> reportHistogram(TaggedMetricRegistry.getBaseName(name), value));
    meters.forEach((name, value) -> reportMetered(TaggedMetricRegistry.getBaseName(name), value));
    timers.forEach((name, value) -> reportTimer(TaggedMetricRegistry.getBaseName(name), value));
  }

  protected void reportCounter(String key, Counter value){
    statsDClient.count(key, value.getCount(), eventTags(value));
  }

  protected void reportTimer(String key, Timer value){
    reportSampling(key, value);
  }

  protected void reportHistogram(String key, Histogram value){
    reportSampling(key, value);
  }

  protected void reportMetered(String key , Metered value ){
    statsDClient.histogram(MetricRegistry.name(key, "count"), value.getCount(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "mean"), value.getMeanRate(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "m1_rate"), value.getOneMinuteRate(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "m5_rate"), value.getFiveMinuteRate(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "m15_rate"), value.getFifteenMinuteRate(), eventTags(value));
  }

  protected void reportGauge(String name, Gauge gauge){
    Object value = gauge.getValue();
    if (value instanceof Long){
      statsDClient.recordGaugeValue(MetricRegistry.name(name), (Long) value, eventTags(gauge));
    } else if (value instanceof Double){
      statsDClient.recordGaugeValue(MetricRegistry.name(name), (Double) value, eventTags(gauge));
    } else if (value instanceof Integer) {
      statsDClient.recordGaugeValue(MetricRegistry.name(name), ((Integer) value).longValue(), eventTags(gauge));
    } else if (value instanceof Float){
      statsDClient.recordGaugeValue(MetricRegistry.name(name), ((Float) value).doubleValue(), eventTags(gauge));
    } else {
      logger.warn("gauge is not supported for type: " + value.getClass().getSimpleName() + " for name: " + name);
    }
  }

  protected void reportSampling(String key, Sampling value){
    Snapshot snapshot = value.getSnapshot();
    statsDClient.histogram(MetricRegistry.name(key, "max"), snapshot.getMax(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "min"), snapshot.getMin(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "mean"), snapshot.getMean(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "stddev"), snapshot.getStdDev(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p50"), snapshot.getMedian(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p75"), snapshot.get75thPercentile(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p95"), snapshot.get95thPercentile(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p98"), snapshot.get98thPercentile(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p99"), snapshot.get99thPercentile(), eventTags(value));
    statsDClient.histogram(MetricRegistry.name(key, "p999"), snapshot.get999thPercentile(), eventTags(value));
  }

  String[] tagMapToStrings(Map<String, String> tags){
    if (tags == null) return new String[0];
    String[] ret = new String[tags.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : tags.entrySet()){
      ret[i++] = entry.getKey() + ":" + entry.getValue();
    }
    return ret;
  }

  String[] eventTags(Object value){
    Map<String, String> tagsToUse = new HashMap<String, String>(tags);
    if (value instanceof TaggedMetric){
      TaggedMetric taggedMetric = (TaggedMetric) value;
      if (taggedMetric.getTags() != null) tagsToUse.putAll(taggedMetric.getTags());
    }
    return tagMapToStrings(tagsToUse);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private Map<String, String> tags;
    private String prefix;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }


    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Append tags to all reported metrics
     *
     * @param tags
     * @return
     */
    public Builder withTags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }


    /**
     * Builds a {@link DogStatsDReporter} with the given properties, sending metrics using the
     * given {@link com.github.sps.metrics.opentsdb.OpenTsdb} client.
     *
     * @param statsDClient a {@link StatsDClient} client
     * @return a {@link DogStatsDReporter}
     */
    public DogStatsDReporter build(StatsDClient statsDClient) {
      return new DogStatsDReporter(registry,
        filter,
        rateUnit,
        durationUnit,
        statsDClient,
        tags,
        prefix);
    }
  }

}
