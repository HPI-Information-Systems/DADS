package de.hpi.msc.jschneider.fileHandling;

import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.utility.Serialize;
import lombok.val;

public class MockSequenceWriter implements SequenceWriter
{
    private double[] values = new double[0];

    public double[] getValues()
    {
        return values;
    }

    @Override
    public void write(double[] records)
    {
        val newValues = new double[values.length + records.length];
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
        write(Serialize.toDoubles(part));
    }

    @Override
    public void close()
    {
    }
}
