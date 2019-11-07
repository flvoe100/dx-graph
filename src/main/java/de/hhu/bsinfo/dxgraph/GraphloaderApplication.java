package de.hhu.bsinfo.dxgraph;


import de.hhu.bsinfo.dxgraph.formats.LDBCFormat;
import de.hhu.bsinfo.dxgraph.model.SimpleEdge;
import de.hhu.bsinfo.dxgraph.model.SimpleVertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


/**
 * "Hello world" example DXRAM application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class GraphloaderApplication extends Application {

    private final Logger LOGGER = LogManager.getFormatterLogger(GraphloaderApplication.class);

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "Dx-Graph";
    }

    @Override
    public void main(final String[] p_args) {
        LOGGER.info("Started Dx-Graph");
        int i = 0;
        String filesDirectoryPath = p_args[i++];
        String datasetName = p_args[i++];

        LDBCFormat format = new LDBCFormat(filesDirectoryPath, datasetName);

        DxGraph graph = new DxGraph(this, format, true);

        graph.loadGraph();

        BootService bootService = this.getService(BootService.class);
        ChunkService chunkService = this.getService(ChunkService.class);
        short ownID = bootService.getNodeID();
        List<Short> peers = bootService.getOnlinePeerNodeIDs();

        for (short nodeID : peers) {
            if (ownID != nodeID) {
                SimpleVertex v = new SimpleVertex();
                v.setID(ChunkID.getChunkID(nodeID, 702));
                chunkService.get().get(v);
                System.out.println("Get on master");
                System.out.println("v.getExtID() = " + v.getExtID());
                System.out.println("ChunkID.getLocalID(v.getID()) = " + ChunkID.getLocalID(v.getID()));
                SimpleEdge e = new SimpleEdge();
                e.setID(ChunkID.getChunkID(nodeID, 9188));
                chunkService.get().get(e);
                System.out.println("Get on master edge 1");
                System.out.println("e.getSinkID() = " + e.getSinkID());
                System.out.println("e.getSourceID() = " + e.getSourceID());
                e = new SimpleEdge();
                e.setID(ChunkID.getChunkID(nodeID, 9189));
                chunkService.get().get(e);
                System.out.println("Get on master edge 4");
                System.out.println("e.getSinkID() = " + e.getSinkID());
                System.out.println("e.getSourceID() = " + e.getSourceID());
            }
        }

        System.exit(0);
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
