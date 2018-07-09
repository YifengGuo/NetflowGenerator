package stream_processing;

import model.Packet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Date;

/**
 * @author yifengguo
 * to map the input line to a tuple2<Packet, 1> object
 * Packet is a wrapper class for netflow's key with its processing time
 */
public class NetflowMapFunction extends RichMapFunction<String, Tuple2<Packet, Integer>> {
    @Override
    public Tuple2<Packet, Integer> map(String line) throws Exception {
        String[] tokens = line.split(" ");
        String srcIp = tokens[1];
        String srcPort = tokens[2];
        String desIp = tokens[3];
        String desPort = tokens[4];
        Packet packet = new Packet(srcIp, srcPort, desIp, desPort, new Date());
        return new Tuple2<>(packet, 1);
    }
}
