package de.hhu.bsinfo.dxgraph.formats;

import de.hhu.bsinfo.dxgraph.LDBC.LDBCPropertiesLoader;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LDBCFormat extends Format {
    public static final String VERTEX_FILE_POSTFIX = "v";
    public static final String EDGE_FILE_POSTFIX = "e";
    public static final String PROP_FILE_POSTFIX = "properties";
    private static final String SPLITTER = ".";

    private String m_directoryPath;
    private String m_datasetName;


    public LDBCFormat(String p_directoryPath, String p_datasetName) {
        super(true, true, LDBCPropertiesLoader.class);
        this.m_directoryPath = p_directoryPath;
        this.m_datasetName = p_datasetName;
    }

    public LDBCFormat(boolean hasVertexFile, boolean hasPropertiesFile, String m_directoryPath, String m_datasetName) {
        super(hasVertexFile, hasPropertiesFile, LDBCPropertiesLoader.class);
        this.m_directoryPath = m_directoryPath;
        this.m_datasetName = m_datasetName;
    }

    @Override
    public Path getVertexFilePath() {
        System.out.println(this.m_datasetName);
        System.out.println("m_directoryPath = " + m_directoryPath);
        return Paths.get(this.m_directoryPath, this.m_datasetName + SPLITTER + VERTEX_FILE_POSTFIX);
    }

    @Override
    public Path getEdgeFilePath() {
        return Paths.get(this.m_directoryPath, this.m_datasetName + SPLITTER + EDGE_FILE_POSTFIX);
    }

    @Override
    public Path getPropertiesFilePath() {
        return Paths.get(this.m_directoryPath, this.m_datasetName + SPLITTER + PROP_FILE_POSTFIX);
    }

}

