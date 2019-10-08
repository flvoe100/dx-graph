package de.hhu.bsinfo.dxgraphloader.formats;


import de.hhu.bsinfo.dxgraphloader.model.FileLoader;
import de.hhu.bsinfo.dxram.engine.ServiceProvider;

import java.nio.file.Path;

public abstract class Format {
    private boolean hasVertexFile;
    private boolean hasPropertiesFile;
    private Class propertiesLoader;
    private Class verticesJobLoader;
    private Class edgeLoader;

    public Format(boolean hasVertexFile, boolean hasPropertiesFile, Class propertiesLoader, Class vertexLoader, Class edgeLoader) {
        this.hasVertexFile = hasVertexFile;
        this.hasPropertiesFile = hasPropertiesFile;
        this.propertiesLoader = propertiesLoader;
        this.verticesJobLoader = vertexLoader;
        this.edgeLoader = edgeLoader;
    }

    public boolean hasVertexFile() {
        return hasVertexFile;
    }

    public boolean hasPropertiesFile() {
        return hasPropertiesFile;
    }

    public FileLoader getPropertiesLoader(ServiceProvider p_context) {
        try {
            return (FileLoader) this.propertiesLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public FileLoader getVertexLoader(ServiceProvider p_context) {
        try {
            return (FileLoader) this.verticesJobLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public FileLoader getEdgeLoader(ServiceProvider p_context) {
        try {
            return (FileLoader) this.edgeLoader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    abstract public Path getPropertiesFilePath();

    abstract public Path getVertexFilePath();

    abstract public Path getEdgeFilePath();
}
