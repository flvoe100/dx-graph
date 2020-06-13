package de.hhu.bsinfo.dxgraph.LDBC;

import de.hhu.bsinfo.dxgraph.model.Edge;
import de.hhu.bsinfo.dxgraph.model.Partition;
import de.hhu.bsinfo.dxgraph.model.PartitionPartition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class LDBCEdgePreprocessThread extends Thread {
    private String path;
    private ArrayBlockingQueue<List<Edge>> queue;
    private Partition m_partition;
    private List<Partition> m_partitions;
    private PartitionPartition m_pp;
    private short m_nodeID;
    private boolean m_isDirected;
    private int m_producerNumber;

    private static final int BATCH_SIZE = 1_000_000;

    public LDBCEdgePreprocessThread(String path, ArrayBlockingQueue<List<Edge>> queue, Partition m_partition, List<Partition> p_partitions, PartitionPartition m_pp, boolean p_isDirected, short p_nodeID, int p_producerNumber) {
        this.path = path;
        this.queue = queue;
        this.m_partition = m_partition;
        this.m_pp = m_pp;
        this.m_nodeID = p_nodeID;
        this.m_partitions = p_partitions;
        this.m_isDirected = p_isDirected;
        this.m_producerNumber = p_producerNumber;
    }

    @Override
    public void run() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            ArrayList<Edge> edgeBatch = new ArrayList<>(BATCH_SIZE);
            String line;
            long parsingDuration = 0;
            long addingDuration = 0;
            long stepDuration = 0;
            int i = 0;
            long start = System.nanoTime();
            br.skip(m_pp.getFromByteOffset());

            while ((line = br.readLine()) != null) {
                i++;
                long stepStart = System.nanoTime();
                if (line.equals(m_pp.getTo())) {
                    queue.put(edgeBatch);
                    break;
                }
                long parsingStart = System.nanoTime();

                String[] split = line.split("\\s");
                long from = Long.parseLong(split[0]);
                long to = Long.parseLong(split[1]);
                parsingDuration += System.nanoTime() - parsingStart;
                long addingStart = System.nanoTime();

                if (m_partition.isBetween(from)) {

                    edgeBatch.add(new Edge(from, to));

                }
                if (!m_isDirected && m_partition.isBetween(to)) {
                    edgeBatch.add(new Edge(to, from));

                }
                if (edgeBatch.size() >= BATCH_SIZE) {
                    queue.put(edgeBatch);
                    edgeBatch = new ArrayList<>(BATCH_SIZE);
                }
                addingDuration += System.nanoTime() - addingStart;
                stepDuration += System.nanoTime() - stepStart;
            }
            queue.put(edgeBatch);
            br.close();
            long duration = System.nanoTime() - start;
            System.out.println("duration = " + duration);
            double avgStepDuration = (double) stepDuration / i;
            double avgParsingDuration =(double) parsingDuration / i;
            double avgAddingDuration = (double)addingDuration / i;
            System.out.println("Reading edges avgStepDuration = " + avgStepDuration);
            System.out.println("Reading edges avgParsingDuration = " + avgParsingDuration);
            System.out.println("Reading edges avgAddingDuration = " + avgAddingDuration);
            System.out.println("Reading edges i = " + i);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
