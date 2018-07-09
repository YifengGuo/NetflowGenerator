package stream_processing;

import model.Packet;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author yifengguo
 * to reduce the KeyedStream partitioned by {@link NetflowKeySelector}
 * reduce the KeyedStream packet with its count to build a netflow
 * and always keep the order of the stream based on its process time
 */
public class NetflowReduceFunction implements ReduceFunction<Tuple2<Packet, Integer>> {
    public Tuple2<Packet, Integer> reduce(Tuple2<Packet, Integer> t1, Tuple2<Packet, Integer> t2) throws Exception {
        Packet packet;
        // keep the stream with latest process time
        if (t1.f0.getProcessTime().getTime() > t2.f0.getProcessTime().getTime()) {
            packet = t1.f0;
        } else {
            packet = t2.f0;
        }
        return new Tuple2<>(packet, t1.f1 + t2.f1);
    }
}
