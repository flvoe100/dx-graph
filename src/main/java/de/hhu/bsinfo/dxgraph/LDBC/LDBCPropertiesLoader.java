package de.hhu.bsinfo.dxgraph.LDBC;


import de.hhu.bsinfo.dxgraph.model.FileLoader;
import de.hhu.bsinfo.dxgraph.model.Graph;
import de.hhu.bsinfo.dxgraph.model.GraphLoadingMetaData;
import de.hhu.bsinfo.dxgraph.model.VerticesTaskResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCPropertiesLoader extends FileLoader {

    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.directed = ";

    private static final Logger LOGGER = LogManager.getFormatterLogger(LDBCPropertiesLoader.class.getSimpleName());

    public LDBCPropertiesLoader() {
    }

    @Override
    public void readFile(Path p_file, Graph p_graph) {
        try {
            Files.lines(p_file)
                    .filter(line -> line.contains(PREFIX_NUM_OF_VERTICES) || line.contains(PREFIX_NUM_OF_EDGES) || line.contains(PREFIX_IS_DIRECTED))
                    .forEach(relevantLines -> {
                        if (relevantLines.contains(PREFIX_NUM_OF_VERTICES)) {
                            String[] split = relevantLines.split(PREFIX_NUM_OF_VERTICES)[1].split("\\s");
                            int[] numberOfVerticesPerSlave = new int[split.length];
                            for (int i = 0; i < split.length; i++) {
                                numberOfVerticesPerSlave[i] = Integer.parseInt(split[i]);
                            }
                            p_graph.setNumberOfVertices(numberOfVerticesPerSlave);
                        }
                        if (relevantLines.contains(PREFIX_NUM_OF_EDGES)) {
                            String[] split = relevantLines.split(PREFIX_NUM_OF_EDGES)[1].split("\\s");
                            int[] numberOfEdgesPerSlave = new int[split.length];
                            for (int i = 0; i < split.length; i++) {
                                numberOfEdgesPerSlave[i] = Integer.parseInt(split[i]);
                            }
                            p_graph.setNumberOfEdges(numberOfEdgesPerSlave);
                        }
                        if (relevantLines.contains(PREFIX_IS_DIRECTED)) {
                            p_graph.setIsDirected(Boolean.parseBoolean(relevantLines.split(PREFIX_IS_DIRECTED)[1]));
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public VerticesTaskResponse readVerticesFile(Path p_filePath, Graph p_graph) {
        return null;
    }



}
