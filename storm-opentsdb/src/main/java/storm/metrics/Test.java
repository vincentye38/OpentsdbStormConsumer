package storm.metrics;

import com.github.sps.metrics.opentsdb.OpenTsdb;
import com.github.sps.metrics.opentsdb.OpenTsdbMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vincenty on 5/23/16.
 */
public class Test {
  public static void main(String[] args){
//    OpenTsdb db = OpenTsdb.forService("http://ec2-52-25-2-75.us-west-2.compute.amazonaws.com:4242").create();
//    Map<String, String> tags = new HashMap<String, String>();
//    tags.put("host", "1");
//    OpenTsdbMetric metric = OpenTsdbMetric.named("testing.metric1").withTags(tags).withTimestamp(System.currentTimeMillis())
//        .withValue(new Integer(1)).build();
//    db.send(metric);
    System.out.println("a: b".replace(':', '_').replace(' ', '_'));
  }
}
