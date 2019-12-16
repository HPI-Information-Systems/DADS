package de.hpi.msc.jschneider.math;

import lombok.val;
import org.ojalgo.matrix.PrimitiveMatrix;

import java.util.stream.DoubleStream;

public class Calculate
{
    private static PrimitiveMatrix filledVector(long length, double value)
    {
        return PrimitiveMatrix.FACTORY.rows(DoubleStream.generate(() -> value).limit(length).toArray());
    }

    public static PrimitiveMatrix transposedColumnMeans(PrimitiveMatrix input)
    {
        val numberOfRows = input.countRows();
        val factor = 1.0d / numberOfRows;
        val e = filledVector(numberOfRows, factor);
        return input.multiply(e.transpose());
    }

    public static PrimitiveMatrix columnCenteredDataMatrix(PrimitiveMatrix input)
    {
        val numberOfRows = input.countRows();
        val e = filledVector(numberOfRows, 1.0d);
        val transposedMeans = transposedColumnMeans(input);
        return input.subtract(transposedMeans.multiply(e));
    }
}
