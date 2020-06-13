package de.hhu.bsinfo.dxgraph.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Edge {

    private long m_sourceID;
    private long m_destID;

    public Edge() {
    }

    public Edge( long p_sourceID, long p_destID) {
        this.m_sourceID = p_sourceID;
        this.m_destID = p_destID;
    }


    public long getSourceID() {
        return  m_sourceID;
    }

    public long getDestID() {
        return  m_destID;
    }
}
