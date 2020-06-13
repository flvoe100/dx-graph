package de.hhu.bsinfo.dxgraph.model;

import java.util.List;

public class Partition {
    private long from;
    private long fromByteOffset;
    private long to;
    private long toByteOffset;
    private int partitionNumber;
    private short nodeID;

    public Partition(long from, long fromByteOffset, long to, long toByteOffset, int partitionNumber, short nodeID) {
        this.from = from;
        this.fromByteOffset = fromByteOffset;
        this.to = to;
        this.toByteOffset = toByteOffset;
        this.partitionNumber = partitionNumber;
        this.nodeID = nodeID;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public long getFromByteOffset() {
        return fromByteOffset;
    }

    public short getNodeID() {
        return nodeID;
    }

    public static short getNodeIDOfVertix(List<Partition> partitions, long vertexID) {
        for (Partition p : partitions) {
            if (p.isBetween(vertexID)) {
                return p.getNodeID();
            }
        }
        return -1;

    }

    public long getToByteOffset() {
        return toByteOffset;
    }

    public boolean isBetween(long x) {
        return x >= from && x <= to;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "from=" + from +
                ", fromByteOffset=" + fromByteOffset +
                ", to=" + to +
                ", toByteOffset=" + toByteOffset +
                ", partitionNumber=" + partitionNumber +
                '}';
    }
}
