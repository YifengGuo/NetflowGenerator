package stream_processing;

import model.Packet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yifengguo
 * main function which glues all the helper functions for processing streams
 */
public class NetflowStreaming {
    public static void start(String inputDataSet, String outputDir, int timeout_minutes, int threshold) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Tuple2<Packet, Integer>> streamSink = env
                .readTextFile(inputDataSet)
                .map(new NetflowMapFunction())
                .keyBy(new NetflowKeySelector())
                .reduce(new NetflowReduceFunction())
                .addSink(new NetflowSinkFunction(outputDir, timeout_minutes, threshold));

        streamSink.setParallelism(1);

        env.execute("netflow generator");
    }
}
