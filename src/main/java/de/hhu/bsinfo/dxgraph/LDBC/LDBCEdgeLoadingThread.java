package de.hhu.bsinfo.dxgraph.LDBC;

import de.hhu.bsinfo.dxgraph.model.*;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;


public class LDBCEdgeLoadingThread extends Thread {
    private ArrayBlockingQueue<List<Edge>> m_edgeQueue;
    private HashMap<Long, Vertex> m_idToVertexMap;
    private List<Partition> m_partitions;
    private ChunkService m_chunkService;
    private ChunkLocalService m_chunkLocalService;
    private short m_nodeID;

    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgeLoadingThread.class.getSimpleName());

    public LDBCEdgeLoadingThread(ArrayBlockingQueue<List<Edge>> p_edgeQueue, List<Partition> p_partitions, HashMap<Long, Vertex> vertexHashMap, ChunkService p_chunkService, ChunkLocalService p_chunkLocalService, short p_nodeID) {
        this.m_edgeQueue = p_edgeQueue;
        this.m_partitions = p_partitions;
        this.m_chunkService = p_chunkService;
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_nodeID = p_nodeID;
        this.m_idToVertexMap = vertexHashMap;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        HashMap<Long, List<Long>> map = new HashMap<>();
        long processedEdges = 0;
        long waitingTime = 120;
        long fillingMapDuration = 0;
        long savingDuration = 0;
        long stepDuration = 0;
        int i = 0;
        while (true) {
            try {
                if (processedEdges > 0) {
                    waitingTime = 15;
                }
                List<Edge> edgeBatch = m_edgeQueue.poll(waitingTime, TimeUnit.SECONDS);
                if (edgeBatch == null) {
                    break;
                }
                i++;
                long startStep = System.nanoTime();
                long startFillingMap = System.nanoTime();
                for (Edge e : edgeBatch) {
                    if (map.containsKey(e.getSourceID())) {
                        List<Long> neighbours = map.get(e.getSourceID());
                        neighbours.add(e.getDestID());
                        map.put(e.getSourceID(), neighbours);
                    } else {
                        ArrayList<Long> neighbours = new ArrayList<>();
                        neighbours.add(e.getDestID());
                        map.put(e.getSourceID(), neighbours);
                    }
                }
                fillingMapDuration += System.nanoTime() - startFillingMap;
                long startSaving = System.nanoTime();
                loadEdgesIntoSystem(map);
                savingDuration += (System.nanoTime() - startSaving);
                processedEdges += edgeBatch.size();
                if (processedEdges % 10_000_000 == 0) {
                    System.out.println(String.format("Processed %d edges", processedEdges));
                }
                map = new HashMap<>();
                stepDuration += (System.nanoTime() - startStep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long duration = System.nanoTime() - start;
        System.out.println("Duration to load graph into system = " + duration);
        System.out.println(String.format("Processed %d edges", processedEdges));
        double avgStepDuration = (double) stepDuration / i;
        double avgFillingDuration = (double) fillingMapDuration / i;
        double avgSavingDuration = (double) savingDuration / i;
        System.out.printf("Consume edges avgStepDuration = %f\n", avgStepDuration);
        System.out.printf("Consume edges avgFillingDuration = %f\n", avgFillingDuration);
        System.out.printf("Consume edges avgSavingDuration = %f\n", avgSavingDuration);
        System.out.println("Consume edges i = " + i);

    }

    public void loadEdgesIntoSystem(HashMap<Long, List<Long>> p_map) {
        //now edges
        Iterator<Map.Entry<Long, List<Long>>> it = p_map.entrySet().iterator();
        SimpleEdge[] neighbours;
        while (it.hasNext()) {
            Map.Entry<Long, List<Long>> entry = it.next();
            DistributedLinkedByteList<SimpleEdge> linkedNeighbourList = null;
            Vertex v = m_idToVertexMap.get(entry.getKey());
            if (v == null) {
                System.err.println(String.format("vertex %d is not in map", entry.getKey()));
            }
            if (v.getNeighbourLinkedList() == null) {
                linkedNeighbourList = DistributedLinkedByteList.create(m_chunkLocalService, m_chunkService, SimpleEdge::new);
                v.setNeighbourLinkedList(linkedNeighbourList);
            } else {
                linkedNeighbourList = v.getNeighbourLinkedList();
            }

            if (linkedNeighbourList == null) {
                System.err.println(String.format("ERROR: No linked neighbour list found for vertice: %d", entry.getKey()));
                return;
            }
            List<Long> neighbourList = entry.getValue();
            neighbours = new SimpleEdge[neighbourList.size()];
            for (int i = 0; i < neighbourList.size(); i++) {
                neighbours[i] = new SimpleEdge(Partition.getNodeIDOfVertix(m_partitions, neighbourList.get(i)));
            }
            linkedNeighbourList.add(m_nodeID, neighbours);
        }
    }
}
