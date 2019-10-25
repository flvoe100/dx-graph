package de.hhu.bsinfo.dxgraph;


import de.hhu.bsinfo.dxgraph.formats.LDBCFormat;
import de.hhu.bsinfo.dxgraph.model.GraphLoadingMetaData;
import de.hhu.bsinfo.dxgraph.model.SimpleVertex;
import de.hhu.bsinfo.dxgraph.util.Util;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;


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
        GraphLoadingMetaData metaData = graph.getMetaData();



        ChunkService chunkService = this.getService(ChunkService.class);
        MasterSlaveComputeService ms = this.getService(MasterSlaveComputeService.class);
        ArrayList<Short> slaves = ms.getStatusMaster().getConnectedSlaves();

        SimpleVertex v = new SimpleVertex();
        short nodeID = -1483;
        v.setID(ChunkID.getChunkID(nodeID, 49467));
        chunkService.get().get(v);

        System.out.println(v.toString());

        this.signalShutdown();
        System.exit(0);
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
