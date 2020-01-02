package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;

public class PrimitiveMatrixSink implements DataSink
{
    private float[] values = new float[0];

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

    public MatrixStore<Double> getMatrix(long numberOfColumns)
    {
        val initializer = new MatrixInitializer(numberOfColumns);
        val numberOfRows = values.length / numberOfColumns;
        for (var rowIndex = 0; rowIndex < numberOfRows; ++rowIndex)
        {
            val row = new float[(int) numberOfColumns];
            for (var columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex)
            {
                row[columnIndex] = values[(int) (rowIndex + columnIndex * numberOfRows)];
            }
            initializer.appendRow(row);
        }

        values = new float[0];
        return initializer.create();
    }
}
