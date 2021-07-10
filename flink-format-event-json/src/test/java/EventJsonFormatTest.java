import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EventJsonFormatTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql(" " +
            " CREATE TABLE sourceTable ( " +
            "  others STRING METADATA FROM 'value.others', " +
            "  key STRING, " +
            "  uid STRING " +
            " ) WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'event', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'properties.enable.auto.commit' = 'false', " +
            "  'properties.session.timeout.ms' = '90000', " +
            "  'properties.request.timeout.ms' = '325000', " +
            "  'scan.startup.mode' = 'earliest-offset' , " +
            "  'value.format' = 'event-json', " +
            "  'value.event-json.others' = 'others' " +
            " ) "
        );

        tableEnvironment.executeSql(" " +
            " CREATE TABLE sinkTable ( " +
            "  others STRING, " +
            "  key STRING, " +
            "  uid STRING " +
            " ) WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'dwd_event', " +
            "  'properties.bootstrap.servers' = '127.0.0.1:9092', " +
            "  'properties.enable.auto.commit' = 'false', " +
            "  'properties.session.timeout.ms' = '90000', " +
            "  'properties.request.timeout.ms' = '325000', " +
            "  'value.format' = 'event-json', " +
            "  'value.event-json.others' = 'others', " +
            "  'sink.partitioner' = 'round-robin', " +
            "  'sink.parallelism' = '4' " +
            " ) "
        );

//        tableEnvironment.executeSql("select * from sourceTable");

        tableEnvironment.executeSql("insert into sinkTable(key, uid, others) select key, uid, others from sourceTable");
    }
}
