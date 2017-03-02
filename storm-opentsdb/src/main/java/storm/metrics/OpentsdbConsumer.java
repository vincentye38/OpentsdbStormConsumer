package storm.metrics;


import com.github.sps.metrics.opentsdb.OpenTsdb;
import com.github.sps.metrics.opentsdb.OpenTsdbMetric;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vincenty on 5/23/16.
 */
public class OpentsdbConsumer implements IMetricsConsumer {
  final static Logger logger = LoggerFactory.getLogger(OpentsdbConsumer.class);

  transient private OpenTsdb db;
  transient private String topology;
  transient private Pattern KafkaOffsetMetricPatter;
  transient private Pattern kafkaPartitionMetricPatter;


  public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
    db = OpenTsdb.forService((String)registrationArgument).create();
    topology = ((String)stormConf.get(Config.TOPOLOGY_NAME)).trim();
    KafkaOffsetMetricPatter = Pattern.compile("(.*)/partition_(\\d*)/(\\w*)");
    kafkaPartitionMetricPatter = Pattern.compile("(Partition\\{host=.*,\\spartition=(\\d*)\\}/)(.*)");
  }

  public void handleDataPoints(TaskInfo taskInfo,
                               Collection<DataPoint> dataPoints) {

    Set<OpenTsdbMetric> metricList = new HashSet<OpenTsdbMetric>();
    Map<String, String> metricAttrs = taskInfoToMap(taskInfo);
    {
      long start = System.currentTimeMillis();
      for (DataPoint dp : dataPoints) {
        ArrayList<String> metricId = new ArrayList<String>();
        metricId.add(topology);
        String name = dp.name;
        metricId.add(name);

        if ("kafkaOffset".equals(name)) {
          Set<OpenTsdbMetric> ret = kafkaOffsetDatapointToMetric(new ArrayList<String>(metricId), new HashMap<String, String>(metricAttrs), dp, taskInfo.timestamp);
          if (ret != null) metricList.addAll(ret);
        } else if ("kafkaPartition".equals(name)) {
          Set<OpenTsdbMetric> ret = kafkaPartitionDatapointToMetrics(new ArrayList<String>(metricId), new HashMap<String, String>(metricAttrs), dp, taskInfo.timestamp);
          if (ret != null) metricList.addAll(ret);
        } else {
          Map<LinkedList<String>, Object> allMetrics = flattenMap(dp);
          for (Map.Entry<LinkedList<String>, Object> metric : allMetrics.entrySet()) {
            metric.getKey().addFirst(topology);
            String metricKey = StringUtils.join(metric.getKey(), '.').replace(':', '_').replace('/', '_').replace(' ', '_');

            Object value = metric.getValue();
            if (value instanceof Number) {
              metricList.add(
                OpenTsdbMetric.named(metricKey)
                  .withTags(metricAttrs)
                  .withTimestamp(taskInfo.timestamp)
                  .withValue(value)
                  .build()
              );
            } else {
              logger.warn("{}'s value type is {}, except Number", name, value.getClass().getSimpleName());
            }

          }
        }

      }
      logger.debug("metrics preparation time: " + (System.currentTimeMillis() - start));
    }
    if (metricList.size() > 0){
      for (OpenTsdbMetric metric : metricList){
        try {
          long start = System.currentTimeMillis();
          db.send(metric);
          logger.debug("send time: " + (System.currentTimeMillis() - start));
        } catch (RuntimeException ex){
          logger.warn("fail to sending metric: {} \n cause: {}" , metric , ex.getMessage());
        }
      }

    }
  }



  Set<OpenTsdbMetric> kafkaPartitionDatapointToMetrics(final ArrayList<String> metricId, final Map<String, String> metricAttrs, DataPoint dataPoint, long timestamp){
    if (dataPoint.value instanceof Map) {
      Set<OpenTsdbMetric> retMetrics = new HashSet<OpenTsdbMetric>();

      Map<String, Object> subMetrics = (Map<String, Object>) dataPoint.value;
      for (Map.Entry<String, Object> entry : subMetrics.entrySet()){
        Matcher matcher = kafkaPartitionMetricPatter.matcher(entry.getKey());
        ArrayList<String> newMetricId = new ArrayList<String>(metricId);
        Map<String, String> newMetricAttrs = new HashMap<String, String>(metricAttrs);
        if (matcher.matches()) {
          String partition = matcher.group(2);
          String metricName = matcher.group(3);
          newMetricId.add(metricName);
          newMetricAttrs.put("partition", partition);
        } else {
          newMetricId.add(entry.getKey());
        }
        String metricKey = StringUtils.join(newMetricId, ".");
        Object value = entry.getValue();
        if (value != null && value instanceof Number) {
          retMetrics.add(
              OpenTsdbMetric.named(metricKey)
                  .withTags(newMetricAttrs)
                  .withTimestamp(timestamp)
                  .withValue(value)
                  .build()
          );
        }
      }
      return retMetrics;

    } else {
      return null;
    }
  }


  Set<OpenTsdbMetric> kafkaOffsetDatapointToMetric(ArrayList<String> metricId, Map<String, String> metricAttrs, DataPoint dataPoint, long timestamp){
    if (dataPoint.value instanceof Map) {
      Set<OpenTsdbMetric> retMetrics = new HashSet<OpenTsdbMetric>();

      Map<String, Object> subMetrics = (Map<String, Object>) dataPoint.value;
      for (Map.Entry<String, Object> entry : subMetrics.entrySet()){
        Matcher matcher = KafkaOffsetMetricPatter.matcher(entry.getKey());
        ArrayList<String> newMetricId = new ArrayList<String>(metricId);
        Map<String, String> newMetricAttrs = new HashMap<String, String>(metricAttrs);
        if (matcher.matches()) {
          String topicName = matcher.group(1);
          String partition = matcher.group(2);
          String metricName = matcher.group(3);
          newMetricId.add(topicName);
          newMetricId.add(metricName);
          newMetricAttrs.put("partition", partition);
        } else {
          newMetricId.add(entry.getKey());
        }
        String metricKey = StringUtils.join(newMetricId, ".");
        Object value = entry.getValue();
        if (value != null && value instanceof Number) {
          retMetrics.add(
              OpenTsdbMetric.named(metricKey)
                  .withTags(newMetricAttrs)
                  .withTimestamp(timestamp)
                  .withValue(value)
                  .build()
          );
        }
      }
      return retMetrics;

    } else {
      return null;
    }
  }

  Map<LinkedList<String>, Object> flattenMap(final DataPoint dp){
    Map<LinkedList<String>, Object> ret = new HashMap<LinkedList<String>, Object>();
    Queue<Pair<LinkedList<String>, Object>> workQ = new LinkedList<Pair<LinkedList<String>,Object>>();
    workQ.add(new Pair<LinkedList<String>, Object>(new LinkedList<String>(){{add(dp.name);}}, dp.value));
    while (!workQ.isEmpty()){
      Pair<LinkedList<String>, Object> curr = workQ.poll();
      if (curr.right instanceof Map){
        for (Map.Entry<String, Object> entry : ((Map<String, Object>)curr.right).entrySet()){
          LinkedList<String> key = new LinkedList<String>(curr.lift);
          key.add(entry.getKey().trim());
          workQ.add(new Pair<LinkedList<String>, Object>(key, entry.getValue()));
        }
      } else {
        ret.put(curr.lift, curr.right);
      }
    }
    return ret;
  }

  public static Map<String, String> taskInfoToMap(TaskInfo taskInfo){
    Map<String, String> metricAttrs = new HashMap<String, String>();
    metricAttrs.put("ComponentId", taskInfo.srcComponentId);
    metricAttrs.put("WorkerHost", taskInfo.srcWorkerHost);
    metricAttrs.put("TaskId", Integer.toString(taskInfo.srcTaskId));
    metricAttrs.put("WorkerPort", Integer.toString(taskInfo.srcWorkerPort));
    return metricAttrs;
  }

  public static class Pair<L,R>{
    public Pair(L lift, R right){
      this.lift = lift;
      this.right = right;
    }

    public L lift;
    public R right;
  }

  public void cleanup() {}

}
