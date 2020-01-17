package de.hpi.msc.jschneider.fileHandling;

import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.utility.Serialize;
import lombok.val;

public class MockSequenceWriter implements SequenceWriter
{
    private float[] values = new float[0];

    public float[] getValues()
    {
        return values;
    }

    @Override
    public void write(float[] records)
    {
        val newValues = new float[values.length + records.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        System.arraycopy(records, 0, newValues, values.length, records.length);

        values = newValues;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void write(byte[] part)
    {
        write(Serialize.toFloats(part));
    }

    @Override
    public void close()
    {
    }
}
