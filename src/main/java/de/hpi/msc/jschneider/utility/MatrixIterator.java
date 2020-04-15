package de.hpi.msc.jschneider.utility;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import org.ojalgo.structure.Access1D;

public class MatrixIterator implements DoubleIterator
{
    private final Access1D<Double> matrix;
    private long currentIndex = 0L;

    public MatrixIterator(Access1D<Double> matrix)
    {
        this.matrix = matrix;
    }

    @Override
    public boolean hasNext()
    {
        return currentIndex < matrix.count();
    }

    @Override
    public double nextDouble()
    {
        return matrix.get(currentIndex++);
    }
}
