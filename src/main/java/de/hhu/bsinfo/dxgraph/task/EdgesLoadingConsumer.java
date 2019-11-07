package de.hhu.bsinfo.dxgraph.task;

import de.hhu.bsinfo.dxgraph.model.Pair;
import de.hhu.bsinfo.dxgraph.model.SimpleEdge;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;

public class EdgesLoadingConsumer implements Runnable {
    private BlockingDeque<Pair<Long>> m_vIDsQueue;
    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private CountDownLatch m_countDownLatch;
    private HashMap<Long, ArrayList<Long>> m_neighborLists;
    private int m_numberOfLocalVertices;
    private short m_nodeID;

    private static final Logger LOGGER = LogManager.getFormatterLogger(EdgesLoadingConsumer.class.getSimpleName());
    final int VERTEX_PACKAGE_SIZE = 1_000_000;

    public EdgesLoadingConsumer(BlockingDeque<Pair<Long>> p_vIDsQueue, CountDownLatch p_numberOfEdges, final int p_numberOfLocalVertices, final short p_nodeID, ChunkLocalService p_chunkLocalService, ChunkService p_chunkService) {
        this.m_vIDsQueue = p_vIDsQueue;
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        this.m_countDownLatch = p_numberOfEdges;
        this.m_numberOfLocalVertices = p_numberOfLocalVertices;
        this.m_neighborLists = new HashMap<>(p_numberOfLocalVertices);
        m_nodeID = p_nodeID;

    }

    @Override
    public void run() {
        try {
            LOGGER.info("Start creating edges");
            int processedVid = 0;
            long[] p_cids;
            SimpleEdge e = new SimpleEdge();
            SimpleEdge[] edges;
            m_chunkLocalService.createLocal().writeRingBuffer();
            while (m_countDownLatch.getCount() != 0) {
                int nextPackageSize = m_countDownLatch.getCount() - VERTEX_PACKAGE_SIZE < 0 ? (int) m_countDownLatch.getCount() : VERTEX_PACKAGE_SIZE;
                p_cids = new long[nextPackageSize];

                int successfulCreates = m_chunkLocalService.createLocal().create(p_cids, nextPackageSize, e.sizeofObject(), false, false);

                if (successfulCreates != nextPackageSize) {
                    LOGGER.error("Error: %d vertices were not created", VERTEX_PACKAGE_SIZE - successfulCreates);
                    System.out.println("ERROR edges were not put " + (nextPackageSize - successfulCreates));

                }
                edges = new SimpleEdge[nextPackageSize];

                for (int i = 0; i < nextPackageSize; i++) {
                    Pair<Long> p_vIDs = m_vIDsQueue.take();


                    e = new SimpleEdge(p_cids[i], p_vIDs.getFrom(), p_vIDs.getTo());
                    edges[i] = e;
                    ArrayList<Long> p_neighboursOfSource = m_neighborLists.get(p_vIDs.getFrom());
                /*    if (p_neighboursOfSource == null) {
                        p_neighboursOfSource = new ArrayList<>();
                    }
                    p_neighboursOfSource.add(p_vIDs.getTo());
                    m_neighborLists.put(p_vIDs.getFrom(), p_neighboursOfSource);

                 */
                    processedVid += nextPackageSize;
                    m_countDownLatch.countDown();

                    if (processedVid % VERTEX_PACKAGE_SIZE == 0) {
                        System.out.println(String.format("Took %d edges", processedVid));
                        LOGGER.info("Took %d vertices", processedVid);
                    }
                }

                int successfulPuts = m_chunkService.put().put(edges);


                if (successfulPuts != nextPackageSize) {
                    System.out.println("ERROR edges were not put " + (nextPackageSize - successfulPuts));
                    LOGGER.error("Error: %d vertices were not put", VERTEX_PACKAGE_SIZE - successfulPuts);
                }

            }
            SimpleEdge test = new SimpleEdge();
            test.setID(ChunkID.getChunkID(m_nodeID, 9188));
            m_chunkService.get().get(test);
            System.out.println("GET for edge 1");
            System.out.println("test.getSourceID() = " + test.getSourceID());
            System.out.println("test.getSinkID() = " + test.getSinkID());
            System.out.println(String.format("%d vertices created and put", processedVid));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
