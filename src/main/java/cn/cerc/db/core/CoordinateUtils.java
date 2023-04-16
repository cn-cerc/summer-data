package cn.cerc.db.core;

import java.util.ArrayList;
import java.util.List;

public class CoordinateUtils {
    public static final int MAX_COORDINATE_GROUP_SIZE = 16;

    public static <T> List<List<T>> splitCoordinates(List<T> coordinates) {
        List<List<T>> groups = new ArrayList<>();
        int size = coordinates.size();
        int groupCount = size / MAX_COORDINATE_GROUP_SIZE + 1;
        for (int i = 0; i < groupCount; i++) {
            int startIndex = i * MAX_COORDINATE_GROUP_SIZE;
            int endIndex = Math.min((i + 1) * MAX_COORDINATE_GROUP_SIZE, size);
            if (startIndex >= endIndex) {
                break;
            }
            List<T> group = coordinates.subList(startIndex, endIndex);
            groups.add(group);
        }
        return groups;
    }

    public static <T> List<List<T>> divideList(List<T> sourceList, int groupSize) {
        List<List<T>> dividedList = new ArrayList<>();
        int listSize = sourceList.size();
        int numOfGroups = (listSize + groupSize - 1) / groupSize;

        for (int i = 0; i < numOfGroups; i++) {
            int startIndex = i * groupSize;
            int endIndex = Math.min(startIndex + groupSize, listSize);
            List<T> sublist = sourceList.subList(startIndex, endIndex);
            dividedList.add(sublist);
        }
        return dividedList;
    }

    public static void main(String[] args) {
        List<Integer> coordinates = new ArrayList<>();
        for (int i = 1; i <= 33; i++) {
            coordinates.add(i);
        }

        long startTime = System.nanoTime();
        List<List<Integer>> groups = CoordinateUtils.divideList(coordinates, 16);
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        for (List<Integer> group : groups) {
            System.out.println(group);
        }
        System.out.println("Split coordinates took " + duration + " nanoseconds.");
    }

}
