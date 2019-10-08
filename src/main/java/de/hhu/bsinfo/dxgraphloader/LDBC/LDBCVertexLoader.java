package de.hhu.bsinfo.dxgraphloader.LDBC;

import de.hhu.bsinfo.dxgraphloader.model.FileLoader;
import de.hhu.bsinfo.dxgraphloader.model.Graph;
import de.hhu.bsinfo.dxgraphloader.model.SimpleVertex;
import de.hhu.bsinfo.dxgraphloader.model.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LDBCVertexLoader implements FileLoader {

    private ChunkLocalService m_chunkLocalService;
    private ChunkService m_chunkService;
    private Class m_vertexClass = SimpleVertex.class;

    public LDBCVertexLoader(ChunkLocalService m_chunkLocalService, ChunkService m_chunkService, Class vertexClass) {
        this.m_chunkLocalService = m_chunkLocalService;
        this.m_chunkService = m_chunkService;
        this.m_vertexClass = vertexClass;
    }

    @Override
    public void readFile(Path p_file, Graph p_graph) {
        //TODO: workaround forcreating custom vertex objects
        try {
            Files.lines(p_file)
                    .mapToLong(line -> Long.parseLong(line.split("\\s")[0]))
                    .forEach(vid -> {
                        Vertex vertex = null;
                        try {
                            vertex = (Vertex) m_vertexClass.newInstance();
                            m_chunkLocalService.createLocal().create(vertex, vid); //TODO
                            m_chunkService.put().put(vertex);
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void setChunkLocalService(ChunkLocalService p_chunkLocalService) {
        this.m_chunkLocalService = p_chunkLocalService;
    }

    public void setChunkService(ChunkService p_chunkService) {
        this.m_chunkService = p_chunkService;
    }
}
