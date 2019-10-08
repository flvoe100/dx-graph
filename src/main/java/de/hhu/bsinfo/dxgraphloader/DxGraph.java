package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.formats.Format;
import de.hhu.bsinfo.dxgraphloader.jobs.VerticesLoadingJob;
import de.hhu.bsinfo.dxgraphloader.model.Graph;
import de.hhu.bsinfo.dxgraphloader.model.GraphLoadingMetaData;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;

public class DxGraph {

    private String m_datasetDirectoryPath;
    private Format m_datasetFormat;
    private GraphLoadingMetaData m_metaData;
    private boolean m_fileForEveryNode;

    private short m_masterNodeID;
    private ServiceProvider m_context;

    private Graph m_graph;

    public DxGraph(String datasetDirectoryPath, Format p_datasetFormat, GraphLoadingMetaData p_metaData, ServiceProvider p_context, boolean p_fileForEveryNode) {
        this.m_datasetDirectoryPath = datasetDirectoryPath;
        this.m_datasetFormat = p_datasetFormat;
        this.m_metaData = p_metaData;
        this.m_fileForEveryNode = p_fileForEveryNode;
        this.m_context = p_context;
        this.m_graph = new Graph();
        MasterSlaveComputeService a;

    }

    public void loadGraph() {
        if (m_datasetFormat.hasPropertiesFile()) {
            this.loadProperties();
        }
    }

    private void loadProperties() {

        this.m_datasetFormat.getPropertiesLoader(m_context).readFile(this.m_datasetFormat.getPropertiesFilePath(), m_graph);
    }

    private void loadVertices() {
        VerticesLoadingJob job;
        JobService p_jobService = m_context.getService(JobService.class);
        for (short nodeID : m_metaData.getPeers()) {
            if (m_fileForEveryNode) {

            }
            job = new VerticesLoadingJob(m_datasetFormat.getVertexFilePath(), m_datasetFormat.getVertexLoader(m_context), m_graph);
            if (nodeID == m_masterNodeID) {
                p_jobService.pushJob(job);
            }
            p_jobService.pushJobRemote(job, nodeID);
        }
        if (!p_jobService.waitForAllJobsToFinish()) {
            //error!
        }
    }

    private void loadEdges() {

    }
}
