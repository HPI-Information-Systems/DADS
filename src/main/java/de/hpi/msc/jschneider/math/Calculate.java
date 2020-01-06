package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.stream.DoubleStream;

public class Calculate
{
    public static MatrixStore<Double> makeFilledVector(long length, double value)
    {
        val matrix = PrimitiveMatrix.FACTORY.rows(DoubleStream.generate(() -> value).limit(length).toArray());
        return MatrixStore.PRIMITIVE.makeWrapper(matrix).get();
    }

    public static MatrixStore<Double> makeVector(double... values)
    {
        val matrix = PrimitiveMatrix.FACTORY.rows(values);
        return MatrixStore.PRIMITIVE.makeWrapper(matrix).get();
    }

    public static MatrixStore<Double> makeRotationX(double angle)
    {
        val cos = (float) Math.cos(angle);
        val sin = (float) Math.sin(angle);

        return (new MatrixInitializer(3L)
                        .appendRow(new float[]{1.0f, 0.0f, 0.0f})
                        .appendRow(new float[]{0.0f, cos, -sin})
                        .appendRow(new float[]{0.0f, sin, cos})
                        .create());
    }

    public static MatrixStore<Double> makeRotationY(double angle)
    {
        val cos = (float) Math.cos(angle);
        val sin = (float) Math.sin(angle);

        return (new MatrixInitializer(3L)
                        .appendRow(new float[]{cos, 0.0f, sin})
                        .appendRow(new float[]{0.0f, 1.0f, 0.0f})
                        .appendRow(new float[]{-sin, 0.0f, cos})
                        .create());
    }

    public static MatrixStore<Double> makeRotationZ(double angle)
    {
        val cos = (float) Math.cos(angle);
        val sin = (float) Math.sin(angle);

        return (new MatrixInitializer(3L)
                        .appendRow(new float[]{cos, -sin, 0.0f})
                        .appendRow(new float[]{sin, cos, 0.0f})
                        .appendRow(new float[]{0.0f, 0.0f, 1.0f})
                        .create());
    }

    public static MatrixStore<Double> transposedColumnMeans(MatrixStore<Double> input)
    {
        val numberOfRows = input.countRows();
        val factor = 1.0d / numberOfRows;
        val e = makeFilledVector(numberOfRows, factor);
        return e.multiply(input);
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input)
    {
        return columnCenteredDataMatrix(input, transposedColumnMeans(input));
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input, MatrixStore<Double> transposedColumnMeans)
    {
        val numberOfRows = input.countRows();
        val e = makeFilledVector(numberOfRows, 1.0d).transpose();
        return input.subtract(e.multiply(transposedColumnMeans));
    }

    public static double angleBetween(MatrixStore<Double> a, MatrixStore<Double> b)
    {
        assert a.countRows() == 1 : "Vectors must be in row format!";
        assert b.countRows() == 1 : "Vectors must be in row format!";

        val dotProduct = a.multiply(b.transpose()).get(0);
        val lengthA = Math.sqrt(a.aggregateAll(Aggregator.SUM2));
        val lengthB = Math.sqrt(b.aggregateAll(Aggregator.SUM2));

        return Math.acos(dotProduct / (lengthA * lengthB));
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
