package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ojalgo.structure.Access1D;

public class PrimitiveAccessSource implements DataSource
{
    private static final Logger Log = LogManager.getLogger(PrimitiveAccessSource.class);

    private long currentPosition = 0L;
    private final Access1D<Double> data;

    public PrimitiveAccessSource(Access1D<Double> data)
    {
        this.data = data;
    }

    @Override
    public boolean isAtEnd()
    {
        return currentPosition >= data.count();
    }

    @Override
    public int elementSizeInBytes()
    {
        return Float.BYTES;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        assert maximumPartSize > -1 : "Length < 0!";

        val maximumNumberOfElements = (int) Math.floor(maximumPartSize / (double) elementSizeInBytes());

        val end = Math.min(data.count(), currentPosition + maximumNumberOfElements);
        var actualLength = end - currentPosition;
        val requiredMemory = actualLength * elementSizeInBytes();
        if (requiredMemory > Integer.MAX_VALUE)
        {
            Log.error("Unable to allocate more than Integer.MAX_VALUE bytes at once!");
            actualLength = (long) Math.floor(Integer.MAX_VALUE / (double) elementSizeInBytes());
        }

        val values = Serialize.toBytes(data, currentPosition, currentPosition + actualLength);
        currentPosition += actualLength;

        return values;
    }
}
