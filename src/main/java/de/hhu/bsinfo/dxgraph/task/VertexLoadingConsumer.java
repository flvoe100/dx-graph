package de.hhu.bsinfo.dxgraph.task;

import de.hhu.bsinfo.dxgraph.model.SimpleVertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;

public class VertexLoadingConsumer implements Runnable {
    private BlockingDeque<Long> vidsQueue;
    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private CountDownLatch m_countDownLatch;
    private static final Logger LOGGER = LogManager.getFormatterLogger(VertexLoadingConsumer.class.getSimpleName());
    final int VERTEX_PACKAGE_SIZE = 10_000;

    public VertexLoadingConsumer(BlockingDeque<Long> vidsQueue, CountDownLatch numberOfVertices, ChunkLocalService m_chunkLocalService, ChunkService m_chunkService) {
        this.vidsQueue = vidsQueue;
        this.m_chunkLocalService = m_chunkLocalService;
        this.m_chunkService = m_chunkService;
        this.m_countDownLatch = numberOfVertices;
    }

    @Override
    public void run() {
        try {
            System.out.println("Start creating vertices");

            int processedVid = 0;
            long[] p_cids;
            SimpleVertex[] vertices;
            SimpleVertex v = new SimpleVertex();
            while (m_countDownLatch.getCount() != 0) {
                int nextPackageSize = m_countDownLatch.getCount() - VERTEX_PACKAGE_SIZE < 0 ? (int) m_countDownLatch.getCount() : VERTEX_PACKAGE_SIZE;
                p_cids = new long[nextPackageSize];


                for (int i = 0; i < nextPackageSize; i++) {
                    long vid = vidsQueue.take();
                    processedVid++;
                    m_countDownLatch.countDown();
                    p_cids[i] = vid;

                    if (processedVid % 10_000 == 0) {
                        LOGGER.info("Took %d vertices", processedVid);
                    }
                }
                int successfulCreates = m_chunkLocalService.createLocal().create(p_cids, nextPackageSize, v.sizeofObject(), true, false);

                if (successfulCreates != nextPackageSize) {
                    LOGGER.error("Error: %d vertices were not created", VERTEX_PACKAGE_SIZE - successfulCreates);
                }
                vertices = new SimpleVertex[nextPackageSize];
                for (int i = 0; i < nextPackageSize; i++) {
                    v = new SimpleVertex(p_cids[i], ChunkID.getLocalID(p_cids[i]));
                    vertices[i] = v;
                }
                int successfulPuts = m_chunkService.put().put(vertices);

                if (successfulPuts != nextPackageSize) {
                    LOGGER.error("Error: %d vertices were not put", VERTEX_PACKAGE_SIZE - successfulPuts);
                }

            }
            LOGGER.info("%d vertices created and put", processedVid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
