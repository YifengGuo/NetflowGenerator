package stream_processing;

import model.Packet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author yifengguo
 */
public class NetflowKeySelector implements KeySelector<Tuple2<Packet, Integer>, Packet> {
    public Packet getKey(Tuple2<Packet, Integer> value) throws Exception {
        return value.f0;
    }
}
