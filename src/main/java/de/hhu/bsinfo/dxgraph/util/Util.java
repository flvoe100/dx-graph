package de.hhu.bsinfo.dxgraph.util;

public class Util {

    public static boolean isInInterval(long start, long end, long toCheck) {
        return toCheck >= start && toCheck <= end;
    }
}
