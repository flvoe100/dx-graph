package de.hhu.bsinfo.dxgraph.model;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class SimpleEdge extends Edge {

    public SimpleEdge() {
    }

    public SimpleEdge(long p_chunkID, long m_sourceID, long m_sinkID) {
        super(p_chunkID, m_sourceID, m_sinkID);
    }

    public SimpleEdge(long p_sourceID, long p_sinkID) {
        super(p_sourceID, p_sinkID);
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject();
    }
}