package de.hpi.msc.jschneider;

import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import org.ojalgo.structure.Access2D;

import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Debug
{
    @SneakyThrows
    public static void print(Access2D<Double> matrix, String fileName)
    {
        val writer = createWriter(fileName);
        writer.write(String.format("%1$d x %2$d\n\n", matrix.countRows(), matrix.countColumns()));

        for (var rowIndex = 0L; rowIndex < matrix.countRows(); ++rowIndex)
        {
            val stringBuilder = new StringBuilder();
            for (var columnIndex = 0L; columnIndex < matrix.countColumns(); ++columnIndex)
            {
                stringBuilder.append(matrix.get(rowIndex, columnIndex));
                stringBuilder.append("\t");
            }
            stringBuilder.append("\n");
            writer.write(stringBuilder.toString());
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(GraphEdge[] edges, String fileName)
    {
        val writer = createWriter(fileName);

        for (val edge : Arrays.stream(edges)
                              .sorted(Comparator.comparingInt((GraphEdge edge) -> edge.getFrom().getIntersectionSegment())
                                                .thenComparingInt(edge -> edge.getFrom().getIndex())
                                                .thenComparingInt(edge -> edge.getTo().getIntersectionSegment())
                                                .thenComparingInt(edge -> edge.getTo().getIndex()))
                              .collect(Collectors.toList()))
        {
            writer.write(edge + "\n");
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(GraphNode[] nodes, String fileName)
    {
        val writer = createWriter(fileName);

        for (val node : Arrays.stream(nodes)
                              .sorted(Comparator.comparingInt(GraphNode::getIntersectionSegment)
                                                .thenComparingInt(GraphNode::getIndex))
                              .collect(Collectors.toList()))
        {
            writer.write(node + "\n");
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(IntersectionCollection[] intersectionCollections, String fileName)
    {
        val writer = createWriter(fileName);

        for (val collection : Arrays.stream(intersectionCollections)
                                    .sorted(Comparator.comparingInt(IntersectionCollection::getIntersectionSegment))
                                    .collect(Collectors.toList()))
        {

            val stringBuilder = new StringBuilder();
            stringBuilder.append(collection.getIntersections().size());
            stringBuilder.append("\t[");
            for (val intersection : collection.getIntersections())
            {
                stringBuilder.append(intersection.getSubSequenceIndex())
                             .append("\t");
            }
            stringBuilder.append("]\n");
            writer.write(stringBuilder.toString());
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(Graph graph, String fileName)
    {
        val writer = createWriter(fileName);

        for (val subSequenceIndex : graph.getCreatedEdgesBySubSequenceIndex().keySet()
                                         .stream()
                                         .sorted(Comparator.comparingLong(Long::longValue))
                                         .collect(Collectors.toList()))
        {
            for (val edgeHash : graph.getCreatedEdgesBySubSequenceIndex().get(subSequenceIndex))
            {
                writer.append(graph.getEdges().get(edgeHash).getKey());
                writer.append("\n");
            }
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(List<List<Integer>> edgeCreationOrder, String fileName)
    {
        val writer = createWriter(fileName);

        for (val collection : edgeCreationOrder)
        {
            val stringBuilder = new StringBuilder();
            stringBuilder.append(collection.size());
            stringBuilder.append("\t[");
            for (val value : collection)
            {
                stringBuilder.append(value);
                stringBuilder.append("\t");
            }
            stringBuilder.append("]\n");
            writer.write(stringBuilder.toString());
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(double[] values, String fileName)
    {
        val writer = createWriter(fileName);

        for (val value : values)
        {
            writer.write(value + "\n");
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    private static Writer createWriter(String fileName)
    {
        val file = new File(".debug/" + fileName);
        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
        {
            throw new Exception(String.format("Unable to create directory \"%1$s\"!", file.getParent()));
        }

        file.delete();

        return new PrintWriter(file);
    }
}
