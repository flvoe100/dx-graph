package de.hhu.bsinfo.dxgraph.LDBC;

import de.hhu.bsinfo.dxgraph.model.FileLoader;
import de.hhu.bsinfo.dxgraph.model.Graph;
import de.hhu.bsinfo.dxgraph.model.Partition;
import de.hhu.bsinfo.dxgraph.model.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;


public class LDBCVertexLoader extends FileLoader {

    private ArrayBlockingQueue<List<Long>> buffer;
    private VertexLoadingConsumer consumer;
    private Partition m_partition;
    final int VERTEX_PACKAGE_SIZE = 1_000_000;

    public LDBCVertexLoader() {
    }

    public LDBCVertexLoader(ChunkLocalService p_chunkLocalService, ChunkService p_chunkService, Partition p_partition, short p_nodeID, HashMap<Long, Vertex> p_idToVertexMap) {
        super(p_chunkLocalService, p_chunkService, p_nodeID);
        this.m_chunkLocalService = p_chunkLocalService;
        this.m_chunkService = p_chunkService;
        buffer = new ArrayBlockingQueue<>(5_000_000);
        consumer = new VertexLoadingConsumer(buffer, m_chunkLocalService, m_chunkService, p_idToVertexMap);
        m_partition = p_partition;
    }


    @Override
    public void readFile(Path p_filePath, Graph p_graph) {
        long parsingDuration = 0;
        long addingDuration = 0;
        long stepDuration = 0;
        Thread t1 = new Thread(consumer);
        t1.start();
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(p_filePath, StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            long vid = -1;
            int i = 0;
            ArrayList<Long> batch = new ArrayList<>(VERTEX_PACKAGE_SIZE);
            long start = System.nanoTime();
            while ((line = br.readLine()) != null) {
                long stepStart = System.nanoTime();
                long parsingStart = System.nanoTime();
                vid = Long.parseLong(line.split("\\s")[0]);
                parsingDuration += System.nanoTime() - parsingStart;
                i++;
                long addingStart = System.nanoTime();
                if (m_partition.getTo() < vid) {
                    break;
                }

                if (m_partition.isBetween(vid)) {
                    batch.add(vid);
                    if (batch.size() == VERTEX_PACKAGE_SIZE) {
                        buffer.put(batch);
                        batch = new ArrayList<>(VERTEX_PACKAGE_SIZE);
                    }

                }
                addingDuration += System.nanoTime() - addingStart;
                stepDuration += System.nanoTime() - stepStart;
            }
            if (batch.size() > 0) {
                buffer.add(batch);

            }
            long duration = System.nanoTime() - start;
            System.out.println("Reading and parsing duration = " + duration);
            System.out.println(String.format("Read %d vertices", i));
            double avgStepDuration = (double) stepDuration / i;
            double avgParsingDuration = (double) parsingDuration / i;
            double avgAddingDuration = (double) addingDuration / i;
            System.out.println("Reading vertices avgStepDuration = " + avgStepDuration);
            System.out.println("Reading vertices avgParsingDuration = " + avgParsingDuration);
            System.out.println("Reading vertices avgAddingDuration = " + avgAddingDuration);
            System.out.println("Reading vertices i = " + i);

            br.close();
            t1.join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
