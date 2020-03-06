package de.hpi.msc.jschneider;

import com.google.common.base.Strings;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.LocalIntersection;
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
    private static final boolean IS_ENABLED = false;

    @SneakyThrows
    public static void print(Access2D<Double> matrix, String fileName)
    {
        if (!IS_ENABLED)
        {
            return;
        }

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
        if (!IS_ENABLED)
        {
            return;
        }

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
        if (!IS_ENABLED)
        {
            return;
        }

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
        if (!IS_ENABLED)
        {
            return;
        }

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
    public static void print(NodeCollection[] nodeCollections, String fileName)
    {
        if (!IS_ENABLED)
        {
            return;
        }

        val writer = createWriter(fileName);
        val sortedNodeCollections = Arrays.stream(nodeCollections)
                                          .sorted(Comparator.comparingInt(NodeCollection::getIntersectionSegment))
                                          .collect(Collectors.toList());

        for (var segmentIndex = 0; segmentIndex < sortedNodeCollections.size(); ++segmentIndex)
        {
            for (var nodeIndex = 0; nodeIndex < sortedNodeCollections.get(segmentIndex).getNodes().size(); ++nodeIndex)
            {
                val stringBuilder = new StringBuilder();
                stringBuilder.append("{")
                             .append(segmentIndex)
                             .append("_")
                             .append(nodeIndex)
                             .append("} ")
                             .append(sortedNodeCollections.get(segmentIndex).getNodes().get(nodeIndex).getIntersectionLength())
                             .append("\n");

                writer.write(stringBuilder.toString());
            }
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(Graph graph, String fileName)
    {
        if (!IS_ENABLED)
        {
            return;
        }

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
        if (!IS_ENABLED)
        {
            return;
        }

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
    public static void print(LocalIntersection[] localIntersections, String fileName)
    {
        if (!IS_ENABLED)
        {
            return;
        }

        val writer = createWriter(fileName);

        for (val localIntersection : localIntersections)
        {
            val stringBuilder = new StringBuilder();
            stringBuilder.append("{")
                         .append(localIntersection.getSubSequenceIndex())
                         .append("_")
                         .append(localIntersection.getIntersectionSegment())
                         .append("} ")
                         .append(localIntersection.getIntersectionDistance())
                         .append("\n");

            writer.write(stringBuilder.toString());
        }

        writer.flush();
        writer.close();
    }

    @SneakyThrows
    public static void print(double[] values, String fileName)
    {
        if (!IS_ENABLED)
        {
            return;
        }

        val writer = createWriter(fileName);

        for (val value : values)
        {
            writer.write(value + "\n");
        }

        writer.flush();
        writer.close();
    }

    public static void printProgress(int iteration, int totalIterations, String title)
    {
        if (!IS_ENABLED)
        {
            return;
        }

        val progressBarLength = 100;
        val progression = (double) iteration / totalIterations;
        val percent = String.format("%.3f %%", progression * 100.0d);
        val filledLength = (int) ((progressBarLength * iteration) / (double) totalIterations);
        val bar = Strings.repeat("█", filledLength) + Strings.repeat("-", progressBarLength - filledLength);
        System.out.print(String.format("\r%1$s |%2$s| %3$s",
                                       Strings.padEnd(title, 30, ' '),
                                       bar,
                                       percent));

        if (iteration >= totalIterations)
        {
            System.out.print("\n");
        }
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
