package stream_processing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FirstTestProgram {
    public static volatile int PACKET_COUNT = 1;
    public static volatile int CUR = 0;
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> lines = env
                .setParallelism(1)
                .readTextFile("src/main/dataset/head2Million.netflow")
                .flatMap(new LineSplitter())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(600)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if (CUR > 100) {
                            return;
                        }
                        PrintWriter writer = null;
                        Map<String, Integer> map = new HashMap<>();
                        // int packet_sum = 0;
                        for (Tuple2<String, Integer> packet : values) {
                            if (map.containsKey(packet.f0)) {
                                map.put(packet.f0, map.get(packet.f0) + packet.f1);
                            } else {
                                map.put(packet.f0, packet.f1);
                            }
                        }

                        for (Map.Entry<String, Integer> entry : map.entrySet()) {
                            String key = entry.getKey();
                            int count = entry.getValue();
                            collector.collect(new Tuple2<>(key, count));
                        }
                    }
                });
                //.sum(1);
        lines.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//                if (CUR > 10) {
//                    return;
//                }
                FileWriter writer = new FileWriter("src/main/java/output/output" + CUR++ + ".txt");
                writer.write(value.f0 + "," + value.f1 + "\n");
                //System.out.println(value.toString());
                writer.flush();
                writer.close();
            }
        });

        // lines.print();
        // lines.writeAsText("src/main/java/output/output" + CUR++ + ".txt", FileSystem.WriteMode.OVERWRITE);
       //  lines.writeAsText("src/main/java/output/test.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute("first test");
    }


    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = line.split(" ");
            // String name = tokens[0];
            String src_ip = tokens[1];
            String src_port = tokens[2];
            String dest_ip = tokens[3];
            String dest_port = tokens[4];
            out.collect(new Tuple2<String, Integer>(src_ip + "," + src_port + "," + dest_ip + "," + dest_port,
                    PACKET_COUNT));
        }
    }
}
