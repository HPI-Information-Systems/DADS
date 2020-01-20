package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import lombok.val;
import lombok.var;
import org.ojalgo.structure.Access1D;

import java.nio.ByteBuffer;

public class Serialize
{
    public static final int GRAPH_EDGE_SIZE = (Integer.BYTES /* node intersection segment */ + Integer.BYTES /* node index */) * 2 + Long.BYTES /* edge weight */;

    public static byte[] toBytes(float[] floats)
    {
        return toBytes(floats, 0, floats.length);
    }

    public static byte[] toBytes(float[] floats, long from, long to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= floats.length : "'To' out of range!";

        val range = to - from;
        val size = range * Float.BYTES;

        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        val bytes = new byte[(int) size];
        for (int floatsIndex = (int) from, bytesIndex = 0; floatsIndex < to; ++floatsIndex, bytesIndex += Float.BYTES)
        {
            insertBytes(bytes, bytesIndex, floats[floatsIndex]);
        }

        return bytes;
    }

    public static byte[] toBytes(Access1D<Double> access1D)
    {
        return toBytes(access1D, 0L, access1D.count());
    }

    public static byte[] toBytes(Access1D<Double> access1D, long from, long to)
    {
        val range = to - from;
        val size = range * Float.BYTES;
        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        val bytes = new byte[(int) size];
        var bytesIndex = 0;
        for (var accessIndex = from; accessIndex < to; ++accessIndex)
        {
            insertBytes(bytes, bytesIndex, access1D.get(accessIndex).floatValue());
            bytesIndex += Float.BYTES;
        }

        return bytes;
    }

    private static void insertBytes(byte[] bytes, int bytesIndex, float value)
    {
        val convertedValue = ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
        System.arraycopy(convertedValue, 0, bytes, bytesIndex, convertedValue.length);
    }

    public static float[] toFloats(byte[] bytes)
    {
        return toFloats(bytes, 0, bytes.length);
    }

    public static float[] toFloats(byte[] bytes, int from, int to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= bytes.length : "'To' out of range";

        val range = to - from;
        assert range % Float.BYTES == 0 : "Range % 4 != 0";

        val size = range / Float.BYTES;

        val floats = new float[size];
        for (int floatsIndex = 0, bytesIndex = from; floatsIndex < floats.length; ++floatsIndex, bytesIndex += Float.BYTES)
        {
            insertFloat(floats, floatsIndex, bytes, bytesIndex);
        }

        return floats;
    }

    private static void insertFloat(float[] floats, int floatsIndex, byte[] bytes, int bytesIndex)
    {
        val convertedValue = ByteBuffer.wrap(bytes, bytesIndex, Float.BYTES).getFloat();
        floats[floatsIndex] = convertedValue;
    }

    public static byte[] toBytes(GraphEdge[] edges)
    {
        return toBytes(edges, 0, edges.length);
    }

    public static byte[] toBytes(GraphEdge[] edges, long from, long to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= edges.length : "'To' out of range!";

        val range = to - from;
        val size = range * GRAPH_EDGE_SIZE;
        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        val bytes = new byte[(int) size];
        for (int edgesIndex = (int) from, bytesIndex = 0; edgesIndex < to; ++edgesIndex, bytesIndex += GRAPH_EDGE_SIZE)
        {
            insertBytes(bytes, bytesIndex, edges[edgesIndex]);
        }

        return bytes;
    }

    private static void insertBytes(byte[] bytes, int bytesIndex, GraphEdge value)
    {
        val buffer = ByteBuffer.allocate(GRAPH_EDGE_SIZE)
                               .putInt(value.getFrom().getIntersectionSegment())
                               .putInt(value.getFrom().getIndex())
                               .putInt(value.getTo().getIntersectionSegment())
                               .putInt(value.getTo().getIndex())
                               .putLong(value.getWeight())
                               .array();

        System.arraycopy(buffer, 0, bytes, bytesIndex, buffer.length);
    }

    public static GraphEdge[] toGraphEdges(byte[] bytes)
    {
        return toGraphEdges(bytes, 0, bytes.length);
    }

    public static GraphEdge[] toGraphEdges(byte[] bytes, long from, long to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= bytes.length : "'To' out of range!";

        val range = to - from;
        assert range % GRAPH_EDGE_SIZE == 0 : String.format("Range %% %1$d != 0", GRAPH_EDGE_SIZE);

        val size = range / GRAPH_EDGE_SIZE;

        val edges = new GraphEdge[(int) size];
        for (int edgesIndex = 0, bytesIndex = (int) from; edgesIndex < edges.length; ++edgesIndex, bytesIndex += GRAPH_EDGE_SIZE)
        {
            insertGraphEdge(edges, edgesIndex, bytes, bytesIndex);
        }

        return edges;
    }

    private static void insertGraphEdge(GraphEdge[] edges, int edgesIndex, byte[] bytes, int bytesIndex)
    {
        val buffer = ByteBuffer.wrap(bytes, bytesIndex, GRAPH_EDGE_SIZE);

        val from = GraphNode.builder()
                            .intersectionSegment(buffer.getInt())
                            .index(buffer.getInt())
                            .build();
        val to = GraphNode.builder()
                          .intersectionSegment(buffer.getInt())
                          .index(buffer.getInt())
                          .build();
        val edge = GraphEdge.builder()
                            .from(from)
                            .to(to)
                            .build();
        edge.setWeight(buffer.getLong());

        edges[edgesIndex] = edge;
    }

    public static byte[] toBytes(int[] ints)
    {
        return toBytes(ints, 0, ints.length);
    }

    public static byte[] toBytes(int[] ints, long from, long to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= ints.length : "'To' out of range!";

        val range = to - from;
        val size = range * Integer.BYTES;
        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        val bytes = new byte[(int) size];
        for (int intsIndex = (int) from, bytesIndex = 0; intsIndex < to; ++intsIndex, bytesIndex += Integer.BYTES)
        {
            insertBytes(bytes, bytesIndex, ints[intsIndex]);
        }

        return bytes;
    }

    private static void insertBytes(byte[] bytes, int bytesIndex, int value)
    {
        val buffer = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        System.arraycopy(buffer, 0, bytes, bytesIndex, buffer.length);
    }

    public static int[] toInts(byte[] bytes)
    {
        return toInts(bytes, 0, bytes.length);
    }

    public static int[] toInts(byte[] bytes, long from, long to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= bytes.length : "'To' out of range!";

        val range = to - from;
        assert range % Integer.BYTES == 0 : "Range % 4 != 0";

        val size = range / Integer.BYTES;

        val ints = new int[(int) size];
        for (int intsIndex = 0, bytesIndex = (int) from; intsIndex < ints.length; ++intsIndex, bytesIndex += Integer.BYTES)
        {
            insertInt(ints, intsIndex, bytes, bytesIndex);
        }

        return ints;
    }

    private static void insertInt(int[] ints, int intsIndex, byte[] bytes, int bytesIndex)
    {
        ints[intsIndex] = ByteBuffer.wrap(bytes, bytesIndex, Integer.BYTES).getInt();
    }
}
