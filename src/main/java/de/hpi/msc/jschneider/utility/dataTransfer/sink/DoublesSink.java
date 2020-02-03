package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class DoublesSink implements DataSink
{
    private final List<Double> doubles = new ArrayList<>();

    @Override
    public void write(byte[] part)
    {
        val newDoubles = Serialize.toDoubles(part);
        for (val newDouble : newDoubles)
        {
            doubles.add(newDouble);
        }
    }

    @Override
    public void close()
    {

    }

    public double[] getDoubles()
    {
        return Doubles.toArray(doubles);
    }
}
