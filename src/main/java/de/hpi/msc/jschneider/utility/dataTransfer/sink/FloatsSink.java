package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import com.google.common.primitives.Floats;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class FloatsSink implements DataSink
{
    private final List<Float> floats = new ArrayList<>();

    @Override
    public void write(byte[] part)
    {
        val newFloats = Serialize.toFloats(part);
        for (val newFloat : newFloats)
        {
            floats.add(newFloat);
        }
    }

    @Override
    public void close()
    {

    }

    public float[] getFloats()
    {
        return Floats.toArray(floats);
    }
}
