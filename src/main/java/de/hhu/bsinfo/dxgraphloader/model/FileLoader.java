package de.hhu.bsinfo.dxgraphloader.model;

import java.nio.file.Path;

@FunctionalInterface
public interface FileLoader {
    void readFile(Path p_file, Graph p_graph);
}
