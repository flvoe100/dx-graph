package de.hhu.bsinfo.dxgraph.util;

import de.hhu.bsinfo.dxgraph.model.Partition;
import de.hhu.bsinfo.dxgraph.model.PartitionPartition;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FilePartitioner {


    public static List<Partition> determinePartitions(String filePath, int numberOfPartitions, short nodeID, boolean isDirected) {
        File file = Paths.get(filePath).toFile();
        ArrayList<Partition> partitions = new ArrayList<>();
        String[] split;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            String source = "";
            long fromID = -1;
            long toID;
            long endOffset = -1;
            long startOffset = 0;
            String line;

            for (int i = 0; i < numberOfPartitions; i++) {

                if (i + 1 == numberOfPartitions) {
                    toID = Long.MAX_VALUE;
                    partitions.add(new Partition(fromID, startOffset, toID, raf.length(), i + 1, nodeID));
                } else {
                    if (i == 0) {
                        source = raf.readLine();
                        split = source.split("\\s");
                        fromID = Long.parseLong(split[0]);
                        startOffset = 0;
                    }
                    raf.seek(raf.length() / numberOfPartitions * (i + 1));
                    raf.readLine();
                    line = raf.readLine();
                    split = line.split("\\s");
                    toID = Long.parseLong(split[0]);

                    long tmpSource = -1;
                    while (true) {
                        endOffset = raf.getFilePointer();
                        line = raf.readLine();
                        split = line.split("\\s");
                        tmpSource = Long.parseLong(split[0]);
                        if (tmpSource != toID) {
                            partitions.add(new Partition(fromID, startOffset, toID, endOffset, i + 1, nodeID));
                            //for next
                            fromID = tmpSource;
                            if(isDirected) {
                                startOffset = endOffset;
                            }
                            break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partitions;
    }

    public static List<Partition> determinePartitions2(String filePath, int numberOfPartitions, short nodeID, boolean isDirected) {
        File file = Paths.get(filePath).toFile();
        ArrayList<Partition> partitions = new ArrayList<>();
        String[] split;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            String source;
            long fromID = -1;
            long toID;
            long endOffset;
            long startOffset = 0;
            String line;

            for (int i = 0; i < numberOfPartitions; i++) {

                if (i + 1 == numberOfPartitions) {
                    toID = Long.MAX_VALUE;
                    partitions.add(new Partition(fromID, startOffset, toID, raf.length(), i + 1, nodeID));
                } else {
                    if (i == 0) {
                        source = raf.readLine();
                        split = source.split("\\s");
                        fromID = Long.parseLong(split[0]);
                        startOffset = 0;
                        raf.seek(raf.length() / 3);
                        raf.readLine();
                        line = raf.readLine();
                        split = line.split("\\s");
                        toID = Long.parseLong(split[0]);
                        long tmpSource = -1;
                        while (true) {
                            endOffset = raf.getFilePointer();
                            line = raf.readLine();
                            split = line.split("\\s");
                            tmpSource = Long.parseLong(split[0]);
                            if (tmpSource != toID) {
                                partitions.add(new Partition(fromID, startOffset, toID, endOffset, i + 1, nodeID));
                                //for next
                                fromID = tmpSource;
                                if(isDirected) {
                                    startOffset = endOffset;
                                }
                                break;
                            }
                        }
                        continue;
                    }
                    System.out.println((raf.length() / 3) + (raf.length() - (raf.length() /3)) / (numberOfPartitions -1) * i );
                    raf.seek((raf.length() / 3) + (raf.length() - (raf.length() /3)) / (numberOfPartitions -1) * i );
                    raf.readLine();
                    line = raf.readLine();
                    split = line.split("\\s");
                    toID = Long.parseLong(split[0]);

                    long tmpSource = -1;
                    while (true) {
                        endOffset = raf.getFilePointer();
                        line = raf.readLine();
                        split = line.split("\\s");
                        tmpSource = Long.parseLong(split[0]);
                        if (tmpSource != toID) {
                            partitions.add(new Partition(fromID, startOffset, toID, endOffset, i + 1, nodeID));
                            //for next
                            fromID = tmpSource;
                            if(isDirected) {
                                startOffset = endOffset;
                            }
                            break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partitions;
    }
    public static List<PartitionPartition> determinePartitionPartitions(Partition p, String filePath, int numberOfPartitions, boolean isLastPartition) {
        File file = Paths.get(filePath).toFile();
        ArrayList<PartitionPartition> partitions = new ArrayList<>();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            String source = "";
            String dest;
            long startOffset = -1;
            long endOffset;
            for (int i = 0; i < numberOfPartitions; i++) {
                if (i == 0) {
                    raf.seek(p.getFromByteOffset());
                    startOffset = raf.getFilePointer();
                    source = raf.readLine();

                }

                if (i + 1 == numberOfPartitions && isLastPartition) {
                    partitions.add(new PartitionPartition(source, startOffset, ""+Long.MAX_VALUE, raf.length(), i + 1));
                    break;
                }
                if (i + 1 == numberOfPartitions && !isLastPartition) {
                    raf.seek(p.getToByteOffset());
                } else {
                    raf.seek(p.getToByteOffset() / numberOfPartitions * (i + 1));
                    raf.readLine();
                }
                endOffset = raf.getFilePointer();
                dest = raf.readLine();

                partitions.add(new PartitionPartition(source, startOffset, dest, endOffset, i + 1));

                source = dest;
                startOffset = endOffset;
            }

            return partitions;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
