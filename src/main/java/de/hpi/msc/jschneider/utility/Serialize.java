package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.Primitive64Matrix;
import org.ojalgo.structure.Access1D;
import scala.Array;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class Serialize
{
    public static final int GRAPH_EDGE_SIZE = (Integer.BYTES /* node intersection segment */ + Integer.BYTES /* node index */) * 2 + Long.BYTES /* edge weight */;

    private static int byteBufferSize(int inputArraySize, long from, long to, int elementSizeInBytes)
    {
        assert from >= 0L && from <= inputArraySize : "\"From\" out of range!";
        assert to >= from && to <= inputArraySize : "\"To\" out of range!";

        val range = to - from;
        val size = range * elementSizeInBytes;

        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        return (int) size;
    }

    public static int inPlace(PrimitiveIterator.OfDouble source, byte[] sink)
    {
        val elementSizeInBytes = Double.BYTES;

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        var sinkIndex = 0;
        for (; source.hasNext() && sinkIndex + elementSizeInBytes <= sink.length; sinkIndex += elementSizeInBytes)
        {
            converter.putDouble(0, source.nextDouble());
            Array.copy(converter.array(), 0, sink, sinkIndex, elementSizeInBytes);
        }

        return sinkIndex;
    }

    public static void backInPlace(byte[] source, int sourceLength, DoubleBigList sink)
    {
        val elementSizeInBytes = Double.BYTES;

        assert source.length >= sourceLength : "SourceLength out of range!";
        assert sourceLength % elementSizeInBytes == 0 : "SourceLength % ElementSize != 0!";

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        for (var sourceIndex = 0; sourceIndex < sourceLength; sourceIndex += elementSizeInBytes)
        {
            converter.position(0);
            converter.put(source, sourceIndex, elementSizeInBytes);
            sink.add(converter.getDouble(0));
        }
    }

    public static int backInPlace(byte[] source, int sourceLength, double[] sink)
    {
        val elementSizeInBytes = Double.BYTES;

        assert source.length >= sourceLength : "SourceLength out of range!";
        assert sourceLength % elementSizeInBytes == 0 : "SourceLength % ElementSize != 0!";
        assert sink.length >= sourceLength / elementSizeInBytes : "Sink overflow!";

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        var sinkIndex = 0;
        for (var sourceIndex = 0; sourceIndex < sourceLength; sourceIndex += elementSizeInBytes, ++sinkIndex)
        {
            converter.position(0);
            converter.put(source, sourceIndex, elementSizeInBytes);
            sink[sinkIndex] = converter.getDouble(0);
        }

        return sinkIndex;
    }

    public static long backInPlace(byte[] source, int sourceLength, Primitive64Matrix.DenseReceiver sink, long sinkIndex)
    {
        val elementSizeInBytes = Double.BYTES;

        assert source.length >= sourceLength : "SourceLength out of range!";
        assert sourceLength % elementSizeInBytes == 0 : "SourceLength % ElementSize != 0!";
        assert sinkIndex + (sourceLength / elementSizeInBytes) <= sink.count() : "Sink overflow!";

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        for (var sourceIndex = 0; sourceIndex < sourceLength; sourceIndex += elementSizeInBytes, ++sinkIndex)
        {
            converter.position(0);
            converter.put(source, sourceIndex, elementSizeInBytes);
            sink.set(sinkIndex, converter.getDouble(0));
        }

        return sinkIndex;
    }

    public static int inPlace(Iterator<GraphEdge> source, byte[] sink)
    {
        val elementSizeInBytes = GRAPH_EDGE_SIZE;

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        var sinkIndex = 0;
        for (; source.hasNext() && sinkIndex + elementSizeInBytes <= sink.length; sinkIndex += elementSizeInBytes)
        {
            val value = source.next();

            converter.position(0);
            converter.putInt(value.getFrom().getIntersectionSegment())
                     .putInt(value.getFrom().getIndex())
                     .putInt(value.getTo().getIntersectionSegment())
                     .putInt(value.getTo().getIndex())
                     .putLong(value.getWeight());

            Array.copy(converter.array(), 0, sink, sinkIndex, elementSizeInBytes);
        }

        return sinkIndex;
    }

    public static int backInPlace(byte[] source, int sourceLength, GraphEdge[] sink)
    {
        val elementSizeInBytes = GRAPH_EDGE_SIZE;

        assert source.length >= sourceLength : "SourceLength out of range!";
        assert sourceLength % elementSizeInBytes == 0 : "SourceLength % ElementSize != 0!";
        assert sourceLength / elementSizeInBytes <= sink.length : "Sink overflow!";

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        var sinkIndex = 0;
        for (var sourceIndex = 0; sourceIndex < sourceLength; sourceIndex += elementSizeInBytes, ++sinkIndex)
        {
            converter.position(0);
            converter.put(source, sourceIndex, elementSizeInBytes);
            sink[sinkIndex] = graphEdgeFromConverter(converter);
        }

        return sinkIndex;
    }

    public static void backInPlace(byte[] source, int sourceLength, Int2ObjectMap<GraphEdge> sink)
    {
        val elementSizeInBytes = GRAPH_EDGE_SIZE;

        assert source.length >= sourceLength : "SourceLength out of range!";
        assert sourceLength % elementSizeInBytes == 0 : "SourceLength % ElementSize != 0!";

        val converter = ByteBuffer.allocate(elementSizeInBytes);
        for (var sourceIndex = 0; sourceIndex < sourceLength; sourceIndex += elementSizeInBytes)
        {
            converter.position(0);
            converter.put(source, sourceIndex, elementSizeInBytes);
            val edge = graphEdgeFromConverter(converter);
            sink.put(edge.hashCode(), edge);
        }
    }

    private static GraphEdge graphEdgeFromConverter(ByteBuffer converter)
    {
        val from = GraphNode.builder()
                            .intersectionSegment(converter.getInt(0))
                            .index(converter.getInt(4))
                            .build();
        val to = GraphNode.builder()
                          .intersectionSegment(converter.getInt(8))
                          .index(converter.getInt(12))
                          .build();
        val edge = GraphEdge.builder()
                            .from(from)
                            .to(to)
                            .build();
        edge.setWeight(converter.getLong(16));

        return edge;
    }

    public static byte[] toBytes(double[] doubles)
    {
        return toBytes(doubles, 0, doubles.length);
    }

    public static byte[] toBytes(double[] doubles, long from, long to)
    {
        val elementSizeInBytes = Double.BYTES;
        val byteBufferSize = byteBufferSize(doubles.length, from, to, elementSizeInBytes);

        val bytes = new byte[byteBufferSize];
        for (int doublesIndex = (int) from, bytesIndex = 0; doublesIndex < to; ++doublesIndex, bytesIndex += elementSizeInBytes)
        {
            insertBytes(bytes, bytesIndex, doubles[doublesIndex]);
        }

        return bytes;
    }

    public static byte[] toBytes(Access1D<Double> access1D)
    {
        return toBytes(access1D, 0L, access1D.count());
    }

    public static byte[] toBytes(Access1D<Double> access1D, long from, long to)
    {
        val elementSizeInBytes = Double.BYTES;
        val byteBufferSize = byteBufferSize(access1D.size(), from, to, elementSizeInBytes);

        val bytes = new byte[byteBufferSize];
        var bytesIndex = 0;
        for (var accessIndex = from; accessIndex < to; ++accessIndex)
        {
            insertBytes(bytes, bytesIndex, access1D.get(accessIndex));
            bytesIndex += elementSizeInBytes;
        }

        return bytes;
    }

    private static void insertBytes(byte[] bytes, int bytesIndex, double value)
    {
        val convertedValue = ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
        System.arraycopy(convertedValue, 0, bytes, bytesIndex, convertedValue.length);
    }

    private static int resultBufferSize(int byteArraySize, long from, long to, int elementSizeInBytes)
    {
        assert from >= 0L && from <= byteArraySize : "\"From\" out of range!";
        assert to >= from && to <= byteArraySize : "\"To\" out of range!";

        val range = to - from;
        assert range % elementSizeInBytes == 0 : String.format("Range %% %1$d != 0!", elementSizeInBytes);

        return (int) (range / elementSizeInBytes);
    }

    public static double[] toDoubles(byte[] bytes)
    {
        return toDoubles(bytes, 0, bytes.length);
    }

    public static double[] toDoubles(byte[] bytes, long from, long to)
    {
        val elementSizeInBytes = Double.BYTES;
        val doubleBufferSize = resultBufferSize(bytes.length, from, to, elementSizeInBytes);

        val doubles = new double[doubleBufferSize];
        for (int doublesIndex = 0, bytesIndex = (int) from; doublesIndex < doubles.length; ++doublesIndex, bytesIndex += elementSizeInBytes)
        {
            insertDouble(doubles, doublesIndex, bytes, bytesIndex);
        }

        return doubles;
    }

    private static void insertDouble(double[] doubles, int doublesIndex, byte[] bytes, int bytesIndex)
    {
        val convertedValue = ByteBuffer.wrap(bytes, bytesIndex, Double.BYTES).getDouble();
        doubles[doublesIndex] = convertedValue;
    }

    public static byte[] toBytes(GraphEdge[] edges)
    {
        return toBytes(edges, 0, edges.length);
    }

    public static byte[] toBytes(GraphEdge[] edges, long from, long to)
    {
        val elementSizeInByte = GRAPH_EDGE_SIZE;
        val byteBufferSize = byteBufferSize(edges.length, from, to, elementSizeInByte);

        val bytes = new byte[byteBufferSize];
        for (int edgesIndex = (int) from, bytesIndex = 0; edgesIndex < to; ++edgesIndex, bytesIndex += elementSizeInByte)
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
        val elementSizeInBytes = GRAPH_EDGE_SIZE;
        val edgeBufferSize = resultBufferSize(bytes.length, from, to, elementSizeInBytes);

        val edges = new GraphEdge[edgeBufferSize];
        for (int edgesIndex = 0, bytesIndex = (int) from; edgesIndex < edges.length; ++edgesIndex, bytesIndex += elementSizeInBytes)
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
        val elementSizeInBytes = Integer.BYTES;
        val byteBufferSize = byteBufferSize(ints.length, from, to, elementSizeInBytes);

        val bytes = new byte[byteBufferSize];
        for (int intsIndex = (int) from, bytesIndex = 0; intsIndex < to; ++intsIndex, bytesIndex += elementSizeInBytes)
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
        val elementSizeInBytes = Integer.BYTES;
        val intBufferSize = resultBufferSize(bytes.length, from, to, elementSizeInBytes);

        val ints = new int[intBufferSize];
        for (int intsIndex = 0, bytesIndex = (int) from; intsIndex < ints.length; ++intsIndex, bytesIndex += elementSizeInBytes)
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
