package de.hhu.bsinfo.dxgraph.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;

public abstract class Vertex extends AbstractChunk {

    public Vertex() {
        super();
    }

    public Vertex(long p_chunkID) {
        super(p_chunkID);
    }

    public long getVertexID() {
        return ChunkID.getLocalID(this.getID());
    }

    @Override
    public void exportObject(Exporter p_exporter) {

    }
}
