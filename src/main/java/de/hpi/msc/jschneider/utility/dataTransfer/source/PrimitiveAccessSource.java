package de.hpi.msc.jschneider.utility.dataTransfer.source;

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
    public float[] read(long length)
    {
        val end = Math.min(currentPosition, Math.max(data.count(), currentPosition + length));
        var actualLength = end - currentPosition;
        if (actualLength > Integer.MAX_VALUE)
        {
            Log.error("Unable to allocate more than Integer.MAX_VALUE floats at once!");
            actualLength = Integer.MAX_VALUE;
        }

        val values = new float[(int) actualLength];
        for (var valuesIndex = 0; valuesIndex < values.length; ++valuesIndex)
        {
            values[valuesIndex] = data.get(valuesIndex + currentPosition).floatValue();
        }
        currentPosition += values.length;

        return values;
    }
}
