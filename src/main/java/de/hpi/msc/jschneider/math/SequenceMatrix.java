package de.hpi.msc.jschneider.math;

import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.Primitive64Store;

public class SequenceMatrix implements MatrixStore<Double>
{
    private final long sequenceLength;
    private final double[] values;
    private boolean isTransposed = false;
    private MatrixStore<Double> columnSubtrahends;

    public SequenceMatrix(long sequenceLength, double[] values)
    {
        this.sequenceLength = sequenceLength;
        this.values = values;
    }

    @Override
    public PhysicalStore.Factory<Double, Primitive64Store> physical()
    {
        return Primitive64Store.FACTORY;
    }

    @Override
    public Double get(long l1, long l2)
    {
        val columnIndex = isTransposed ? l1 : l2;
        val rowIndex = isTransposed ? l2 : l1;

        val diff = Math.min(rowIndex, sequenceLength - 1 - columnIndex);
        val valueRowIndex = rowIndex - diff;
        val valueColumnIndex = columnIndex + diff;

        var value = values[(int) (valueColumnIndex + valueRowIndex)];
        if (columnSubtrahends != null)
        {
            value -= columnSubtrahends.get(0, columnIndex);
        }

        return value;
    }

    @Override
    public MatrixStore<Double> transpose()
    {
        val result = myCopy();
        result.isTransposed = !result.isTransposed;

        return result;
    }

    public SequenceMatrix subtractColumnBased(MatrixStore<Double> columnSubtrahends)
    {
        assert columnSubtrahends.countColumns() == countColumns() : "ColumnSubtrahends must have the exact same amount of columns!";
        assert columnSubtrahends.countRows() == 1 : "ColumnSubtrahends must have only 1 row!";

        val result = myCopy();
        if (result.columnSubtrahends != null)
        {
            result.columnSubtrahends = result.columnSubtrahends.add(columnSubtrahends);
        }
        else
        {
            result.columnSubtrahends = columnSubtrahends;
        }

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

    private SequenceMatrix myCopy()
    {
        val copy = new SequenceMatrix(sequenceLength, values);
        copy.isTransposed = isTransposed;
        copy.columnSubtrahends = columnSubtrahends;

        return copy;
    }
}
