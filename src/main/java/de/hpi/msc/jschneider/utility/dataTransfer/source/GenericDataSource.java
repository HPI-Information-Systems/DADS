package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ojalgo.structure.Access1D;
import org.ojalgo.structure.Structure1D;

import java.util.function.Function;

public class GenericDataSource<TData> implements DataSource
{
    private static final Logger Log = LogManager.getLogger(GenericDataSource.class);

    private long currentPosition = 0L;
    private final TData data;
    private final Function<TData, Long> dataSizeProvider;
    private final int elementSizeInBytes;
    private final Serializer<TData> serializer;

    public GenericDataSource(TData data, Function<TData, Long> dataSizeProvider, int elementSizeInBytes, Serializer<TData> serializer)
    {
        this.data = data;
        this.dataSizeProvider = dataSizeProvider;
        this.elementSizeInBytes = elementSizeInBytes;
        this.serializer = serializer;
    }

    public static DataSource create(double[] data)
    {
        return new DoublesDataSource(data);
    }

    public static GenericDataSource<Access1D<Double>> create(Access1D<Double> data)
    {
        return new GenericDataSource<>(data, Structure1D::count, Double.BYTES, Serialize::toBytes);
    }

    public static GenericDataSource<GraphEdge[]> create(GraphEdge[] data)
    {
        return new GenericDataSource<>(data, d -> (long) d.length, Serialize.GRAPH_EDGE_SIZE, Serialize::toBytes);
    }

    public static GenericDataSource<int[]> create(int[] data)
    {
        return new GenericDataSource<>(data, d -> (long) d.length, Integer.BYTES, Serialize::toBytes);
    }

    @Override
    public boolean isAtEnd()
    {
        return currentPosition >= dataSizeProvider.apply(data);
    }

    @Override
    public int elementSizeInBytes()
    {
        return elementSizeInBytes;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        assert maximumPartSize > -1 : "Length < 0!";

        val maximumNumberOfElements = (int) Math.floor(maximumPartSize / (double) elementSizeInBytes());

        val end = Math.min(dataSizeProvider.apply(data), currentPosition + maximumNumberOfElements);
        var actualNumberOfElements = end - currentPosition;
        val requiredMemory = actualNumberOfElements * elementSizeInBytes();
        if (requiredMemory > Integer.MAX_VALUE)
        {
            Log.error("Unable to allocate more than Integer.MAX_VALUE bytes at once!");
            actualNumberOfElements = (long) Math.floor(Integer.MAX_VALUE / (double) elementSizeInBytes());
        }

        val bytes = serializer.serialize(data, currentPosition, currentPosition + actualNumberOfElements);
        currentPosition += actualNumberOfElements;

        return bytes;
    }
}
