package de.hpi.msc.jschneider.utility.dataTransfer;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.Arrays;

public class MockDataSink implements DataSink
{
    private float[] values = new float[0];

    public float[] getValues()
    {
        return values;
    }

    @Override
    public void write(float[] part)
    {
        val newValues = new float[values.length + part.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        System.arraycopy(part, 0, newValues, values.length, part.length);

        values = newValues;
    }

    @Override
    public void close()
    {
    }

    public MatrixStore<Double> toMatrix(long columns)
    {
        val initializer = new MatrixInitializer(columns);
        for (var i = 0L; i < values.length; i += columns)
        {
            initializer.appendRow(Arrays.copyOfRange(values, (int) i, (int) (i + columns - 1)));
        }

        return initializer.create();
    }
}
