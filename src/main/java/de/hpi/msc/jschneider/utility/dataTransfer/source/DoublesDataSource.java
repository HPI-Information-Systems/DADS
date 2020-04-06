package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

public class DoublesDataSource implements DataSource
{
    private static final Logger Log = LogManager.getLogger(GenericDataSource.class);

    private double[] data;

    public DoublesDataSource(double[] data)
    {
        this.data = data;
    }

    @Override
    public boolean isAtEnd()
    {
        return data.length < 1;
    }

    @Override
    public int elementSizeInBytes()
    {
        return Double.BYTES;
    }

    @Override
    public int numberOfElements()
    {
        return data.length;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        assert maximumPartSize > -1 : "Length < 0!";

        val maximumNumberOfElements = (int) Math.floor(maximumPartSize / (double) elementSizeInBytes());

        long actualNumberOfElements = Math.min(data.length, maximumNumberOfElements);
        final long requiredMemory = actualNumberOfElements * elementSizeInBytes();
        if (requiredMemory > Integer.MAX_VALUE)
        {
            Log.error("Unable to allocate more than Integer.MAX_VALUE bytes at once!");
            actualNumberOfElements = (long) Math.floor(Integer.MAX_VALUE / (double) elementSizeInBytes());
        }

        val bytes = Serialize.toBytes(data, 0L, actualNumberOfElements);
        data = Arrays.copyOfRange(data, (int) actualNumberOfElements, data.length);

        return bytes;
    }
}
