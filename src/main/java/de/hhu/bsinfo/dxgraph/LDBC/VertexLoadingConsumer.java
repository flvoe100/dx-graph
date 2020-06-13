package de.hhu.bsinfo.dxgraph.LDBC;

import de.hhu.bsinfo.dxgraph.model.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class VertexLoadingConsumer implements Runnable {
    private ArrayBlockingQueue<List<Long>> m_vIDsQueue;
    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private HashMap<Long, Vertex> m_idToVertexMap;
    private static final Logger LOGGER = LogManager.getFormatterLogger(VertexLoadingConsumer.class.getSimpleName());

    public VertexLoadingConsumer(ArrayBlockingQueue<List<Long>> vidsQueue, ChunkLocalService m_chunkLocalService, ChunkService m_chunkService, HashMap<Long, Vertex> p_idToVertexMap) {
        this.m_vIDsQueue = vidsQueue;
        this.m_chunkLocalService = m_chunkLocalService;
        this.m_chunkService = m_chunkService;
        this.m_idToVertexMap = p_idToVertexMap;
    }


    @Override
    public void run() {

        try {
            System.out.println("Start creating vertices");
            BufferedWriter bw = new BufferedWriter(new FileWriter("/home/voelz/idsToVertex.txt"));
            int processedVid = 0;
            long[] p_cids;
            Vertex[] vertices;
            Vertex v = new Vertex();
            long creatingDuration = 0;
            long puttingDuration = 0;
            long stepDuration = 0;
            int j = 0;
            long start = System.nanoTime();
            while (true) {

                List<Long> ids = m_vIDsQueue.poll(15, TimeUnit.SECONDS);
                if (ids == null) {
                    break;
                }
                j++;
                long startStep = System.nanoTime();

                p_cids = new long[ids.size()];

                for (int i = 0; i < ids.size(); i++) {
                    p_cids[i] = ids.get(i);

                }
                long startCreate = System.nanoTime();
                int successfulCreates = m_chunkLocalService.createLocal().create(p_cids, ids.size(), v.sizeofObject(), true, false);
                if (successfulCreates != ids.size()) {
                    LOGGER.error("Error: %d vertices were not created", ids.size() - successfulCreates);
                }
                creatingDuration += System.nanoTime() - startCreate;
                vertices = new Vertex[p_cids.length];
                for (int i = 0; i < p_cids.length; i++) {
                    v = new Vertex(p_cids[i]);
                    vertices[i] = v;
                    m_idToVertexMap.put(ids.get(i), v);
                }
                long startPut = System.nanoTime();
                int successfulPuts = m_chunkService.put().put(vertices);
                processedVid += successfulPuts;
                if (successfulPuts != ids.size()) {
                    LOGGER.error("Error: %d vertices were not put", ids.size() - successfulPuts);
                }
                puttingDuration += System.nanoTime() - startPut;
                stepDuration += System.nanoTime() - startStep;
            }
            bw.flush();
            bw.close();
            System.out.println(String.format("%d vertices created and put", processedVid));
            long duration = System.nanoTime() - start;
            System.out.println("duration = " + duration);
            double avgStepDuration = (double) stepDuration / j;
            double avgCreateDuration =(double) creatingDuration / j;
            double avgPutDuration =(double) puttingDuration / j;
            System.out.printf("Consume vertices avgStepDuration = %f\n", + avgStepDuration);
            System.out.printf("Consume vertices avgCreateDuration = %f\n", + avgCreateDuration);
            System.out.printf("Consume vertices avgPutDuration = %f\n", + avgPutDuration);
            System.out.println("Consume vertices i = " + j);


        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }


    }

}
