package de.hpi.msc.jschneider.math;

import lombok.val;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PhysicalStore;
import org.ojalgo.matrix.store.Primitive64Store;

public class ConstantVector implements MatrixStore<Double>
{
    private final long numberOfRows;
    private final long numberOfColumns;
    private final double value;
    private boolean isTransposed = false;

    public ConstantVector(long rows, long columns, double value)
    {
        numberOfRows = rows;
        numberOfColumns = columns;
        this.value = value;
    }

    @Override
    public PhysicalStore.Factory<Double, Primitive64Store> physical()
    {
        return Primitive64Store.FACTORY;
    }

    @Override
    public Double get(long row, long col)
    {
        return value;
    }

    @Override
    public long countColumns()
    {
        return isTransposed ? numberOfRows : numberOfColumns;
    }

    @Override
    public long countRows()
    {
        return isTransposed ? numberOfColumns : numberOfRows;
    }

    @Override
    public MatrixStore<Double> transpose()
    {
        val vector = new ConstantVector(numberOfRows, numberOfColumns, value);
        vector.isTransposed = !isTransposed;
        return vector;
    }
}
