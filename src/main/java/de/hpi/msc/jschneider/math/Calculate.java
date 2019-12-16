package de.hpi.msc.jschneider.math;

import lombok.val;
import org.ojalgo.matrix.PrimitiveMatrix;

import java.util.stream.DoubleStream;

public class Calculate
{
    public static PrimitiveMatrix filledVector(long length, double value)
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
        return columnCenteredDataMatrix(input, transposedColumnMeans(input));
    }

    public static PrimitiveMatrix columnCenteredDataMatrix(PrimitiveMatrix input, PrimitiveMatrix transposedColumnMeans)
    {
        val numberOfRows = input.countRows();
        val e = filledVector(numberOfRows, 1.0d);
        return input.subtract(transposedColumnMeans.multiply(e));
    }

    public static double log2(double value)
    {
        return Math.log(value) / Math.log(2);
    }

    public static int nextPowerOfTwo(int value)
    {
        val log = log2(value);
        val logFloor = Math.floor(log);
        if (log - logFloor == log)
        {
            return value;
        }

        return (int) Math.pow(2, Math.ceil(log));
    }
}
