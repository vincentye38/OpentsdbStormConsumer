Build:
mvn clean package.

Deploy storm-metrics-opentsdb-xx-SNAPSHOT-jar-with-dependencies.jar to <storm-installation-directory>/lib on every Storm machine.

add the following to storm.yaml

topology.metrics.consumer.register:
  - class: "storm.metrics.OpentsdbConsumer"
    argument: "http://ec2-52-25-2-75.us-west-2.compute.amazonaws.com:4242"

The value of argument is the url to the opentsdb.