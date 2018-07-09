package model;

import java.util.Date;

/**
 * @author yifengguo
 */
public class Packet {
    private String srcIp;
    private String srcPort;
    private String desIp;
    private String desPort;
    private Date processTime;

    public Packet(String srcIp, String srcPort, String desIp, String desPort, Date processTime) {
        this.srcIp = srcIp;
        this.srcPort = srcPort;
        this.desIp = desIp;
        this.desPort = desPort;
        this.processTime = processTime;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public String getSrcPort() {
        return srcPort;
    }

    public String getDesIp() {
        return desIp;
    }

    public String getDesPort() {
        return desPort;
    }

    public Date getProcessTime() {
        return processTime;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public void setSrcPort(String srcPort) {
        this.srcPort = srcPort;
    }

    public void setDesIp(String desIp) {
        this.desIp = desIp;
    }

    public void setDesPort(String desPort) {
        this.desPort = desPort;
    }

    public void setProcessTime(Date processTime) {
        this.processTime = processTime;
    }

    /**
     * use packet fields src_ip, src_port, des_ip, des_port to
     * compare two Packet Objects
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Packet packet = (Packet) o;

        if (!srcIp.equals(packet.srcIp)) return false;
        if (!srcPort.equals(packet.srcPort)) return false;
        if (!desIp.equals(packet.desIp)) return false;
        return desPort.equals(packet.desPort);
    }

    @Override
    public int hashCode() {
        int result = srcIp.hashCode();
        result = 31 * result + srcPort.hashCode();
        result = 31 * result + desIp.hashCode();
        result = 31 * result + desPort.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return srcIp + "," + srcPort + "," + desIp + "," + desPort;
    }
}
