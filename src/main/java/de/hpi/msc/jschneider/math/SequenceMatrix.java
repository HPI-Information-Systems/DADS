package de.hpi.msc.jschneider.math;

import lombok.val;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;

public class SequenceMatrix implements MatrixStore<Double>
{
    private final long sequenceLength;
    private final double[] values;
    private boolean isTransposed = false;

    public SequenceMatrix(long sequenceLength, double[] values)
    {
        this.sequenceLength = sequenceLength;
        this.values = values;
    }

    @Override
    public PhysicalStore.Factory<Double, ?> physical()
    {
        return null;
    }

    @Override
    public Double get(long l1, long l2)
    {
        if (isTransposed)
        {
            return getRowFirst(l1, l2);
        }
        else
        {
            return getColumnFirst(l1, l2);
        }
    }

    private Double getRowFirst(long rowIndex, long columnIndex)
    {
        val diff = Math.min(columnIndex, sequenceLength - 1 - rowIndex);
        rowIndex += diff;
        columnIndex -= diff;

        return values[(int) (columnIndex + rowIndex)];
    }

    private double getColumnFirst(long columnIndex, long rowIndex)
    {
        val diff = Math.min(columnIndex, sequenceLength - 1 - rowIndex);
        rowIndex += diff;
        columnIndex -= diff;

        return values[(int) (columnIndex + rowIndex)];
    }

    @Override
    public MatrixStore<Double> transpose()
    {
        val result = new SequenceMatrix(sequenceLength, values);
        result.isTransposed = !isTransposed;

        return result;
    }

    @Override
    public long countColumns()
    {
        if (isTransposed)
        {
            return numberOfRows();
        }
        else
        {
            return numberOfColumns();
        }
    }

    @Override
    public long countRows()
    {
        if (isTransposed)
        {
            return numberOfColumns();
        }
        else
        {
            return numberOfRows();
        }
    }

    private long numberOfColumns()
    {
        return sequenceLength;
    }

    private long numberOfRows()
    {
        return values.length - sequenceLength + 1;
    }
}
