package de.hpi.msc.jschneider.math;

import lombok.val;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.stream.DoubleStream;

public class Calculate
{
    public static MatrixStore<Double> filledVector(long length, double value)
    {
        val matrix = PrimitiveMatrix.FACTORY.rows(DoubleStream.generate(() -> value).limit(length).toArray());
        return MatrixStore.PRIMITIVE.makeWrapper(matrix).get();
    }

    public static MatrixStore<Double> transposedColumnMeans(MatrixStore<Double> input)
    {
        val numberOfRows = input.countRows();
        val factor = 1.0d / numberOfRows;
        val e = filledVector(numberOfRows, factor);
        return e.multiply(input);
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input)
    {
        return columnCenteredDataMatrix(input, transposedColumnMeans(input));
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input, MatrixStore<Double> transposedColumnMeans)
    {
        val numberOfRows = input.countRows();
        val e = filledVector(numberOfRows, 1.0d).transpose();
        return input.subtract(e.multiply(transposedColumnMeans));
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
