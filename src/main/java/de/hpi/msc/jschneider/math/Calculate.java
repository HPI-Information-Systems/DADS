package de.hpi.msc.jschneider.math;

import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.structure.Access1D;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.DoubleStream;

public class Calculate
{
    private static final double TWO_PI = 2 * Math.PI;

    public static MatrixStore<Double> makeFilledRowVector(long length, double value)
    {
        val matrix = PrimitiveMatrix.FACTORY.rows(DoubleStream.generate(() -> value).limit(length).toArray());
        return MatrixStore.PRIMITIVE.makeWrapper(matrix).get();
    }

    public static MatrixStore<Double> makeRowVector(double... values)
    {
        val matrix = PrimitiveMatrix.FACTORY.rows(values);
        return MatrixStore.PRIMITIVE.makeWrapper(matrix).get();
    }

    public static double[] makeRange(double start, double end, int numberOfSteps)
    {
        val step = (end - start) / numberOfSteps;
        val range = new double[numberOfSteps];
        for (var i = 0; i < range.length; ++i)
        {
            range[i] = start + i * step;
        }

        return range;
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
        val e = makeFilledRowVector(numberOfRows, factor);
        return e.multiply(input);
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input)
    {
        return columnCenteredDataMatrix(input, transposedColumnMeans(input));
    }

    public static MatrixStore<Double> columnCenteredDataMatrix(MatrixStore<Double> input, MatrixStore<Double> transposedColumnMeans)
    {
        val numberOfRows = input.countRows();
        val e = makeFilledRowVector(numberOfRows, 1.0d).transpose();
        return input.subtract(e.multiply(transposedColumnMeans));
    }

    public static double angleBetween(MatrixStore<Double> a, MatrixStore<Double> b)
    {
        a = toColumnVector(a);
        b = toColumnVector(b);

        val dotProduct = a.multiply(b.transpose()).get(0);
        val lengthA = Math.sqrt(a.aggregateAll(Aggregator.SUM2));
        val lengthB = Math.sqrt(b.aggregateAll(Aggregator.SUM2));

        return Math.acos(dotProduct / (lengthA * lengthB));
    }

    public static IntersectionCollection[] intersections(MatrixStore<Double> reducedProjection, int numberOfSamples)
    {
        // TODO: parallelize execution?! --> split up reduced projection by columns

        assert reducedProjection.countRows() == 2 : "ReducedProjection must have 2 dimensions in column vector format!";
        assert reducedProjection.countColumns() > 1 : "ReducedProjection must have at least 2 records!";

        val radiusX = Math.max(reducedProjection.aggregateRow(0L, Aggregator.MAXIMUM), Math.abs(reducedProjection.aggregateRow(0L, Aggregator.MINIMUM)));
        val radiusY = Math.max(reducedProjection.aggregateRow(1L, Aggregator.MAXIMUM), Math.abs(reducedProjection.aggregateRow(1L, Aggregator.MINIMUM)));
        val radiusLength = Math.sqrt(radiusX * radiusX + radiusY * radiusY);

        val intersectionPoints = makeIntersectionPoints(radiusLength, numberOfSamples);
        val intersectionCollections = new IntersectionCollection[numberOfSamples];
        for (var i = 0; i < intersectionCollections.length; ++i)
        {
            intersectionCollections[i] = IntersectionCollection.builder()
                                                               .intersectionPointIndex(i)
                                                               .build();
        }

        for (var columnIndex = 0; columnIndex < reducedProjection.countColumns() - 1; ++columnIndex)
        {
            val current = reducedProjection.sliceColumn(columnIndex);
            val next = reducedProjection.sliceColumn(columnIndex + 1);

            for (val intersectionPointIndex : intersectionPointIndicesToCheck(current, next, numberOfSamples))
            {
                val intersection = tryCalculateIntersection(intersectionPoints.get(intersectionPointIndex), current, next);
                if (!intersection.isPresent())
                {
                    continue;
                }

                intersectionCollections[intersectionPointIndex].getIntersections().add(intersection.get());
            }
        }

        return intersectionCollections;
    }

    private static List<MatrixStore<Double>> makeIntersectionPoints(double radiusLength, int numberOfSamples)
    {
        val intersectionPoints = new ArrayList<MatrixStore<Double>>(numberOfSamples);
        val angleStep = TWO_PI / numberOfSamples;

        for (var i = 0; i < numberOfSamples; ++i)
        {
            val x = Math.cos(angleStep * i) * radiusLength;
            val y = Math.sin(angleStep * i) * radiusLength;
            intersectionPoints.add(makeRowVector(x, y).transpose());
        }

        return intersectionPoints;
    }

    private static int[] intersectionPointIndicesToCheck(Access1D<Double> current, Access1D<Double> next, int numberOfSamples)
    {
        val currentX = current.get(0);
        val currentY = current.get(1);
        val nextX = next.get(0);
        val nextY = next.get(1);
        val currentLength = Math.sqrt(currentX * currentX + currentY * currentY);
        val nextLength = Math.sqrt(nextX * nextX + nextY * nextY);
        var currentTheta = Math.atan2(currentY / currentLength, currentX / currentLength);
        var nextTheta = Math.atan2(nextY / nextLength, nextX / nextLength);

        if (currentTheta < 0)
        {
            currentTheta += TWO_PI;
        }

        if (nextTheta < 0)
        {
            nextTheta += TWO_PI;
        }

        val currentIntersectionPointIndex = (int) (currentTheta / TWO_PI * numberOfSamples);
        val nextIntersectionPointIndex = (int) (nextTheta / TWO_PI * numberOfSamples);
        var diff = Math.abs(currentIntersectionPointIndex - nextIntersectionPointIndex);
        val halfSamples = numberOfSamples / 2;
        if (diff > halfSamples)
        {
            if (nextIntersectionPointIndex > halfSamples)
            {
                diff = Math.abs(currentIntersectionPointIndex + numberOfSamples - nextIntersectionPointIndex);
            }
            else if (currentIntersectionPointIndex > halfSamples)
            {
                diff = Math.abs(currentIntersectionPointIndex - numberOfSamples - nextIntersectionPointIndex);
            }
        }
        diff = Math.min(diff, halfSamples);
        val intersectionPointIndices = new int[2 * (diff + 1)];
        val intersectionPointOffset = -diff - 1;
        for (var i = 0; i < intersectionPointIndices.length; ++i)
        {
            intersectionPointIndices[i] = Math.floorMod(currentIntersectionPointIndex + intersectionPointOffset + i, numberOfSamples);
        }

        return intersectionPointIndices;
    }

    private static Optional<Intersection> tryCalculateIntersection(Access1D<Double> intersectionPoint, Access1D<Double> current, Access1D<Double> next)
    {
        val origin = makeRowVector(0.0d, 0.0d);

        val line1StartX = origin.get(0);
        val line1StartY = origin.get(1);
        val line1EndX = intersectionPoint.get(0);
        val line1EndY = intersectionPoint.get(1);
        val line1DiffX = line1StartX - line1EndX;
        val line1DiffY = line1StartY - line1EndY;

        val line2StartX = current.get(0);
        val line2StartY = current.get(1);
        val line2EndX = next.get(0);
        val line2EndY = next.get(1);
        val line2DiffX = line2StartX - line2EndX;
        val line2DiffY = line2StartY - line2EndY;

        val diffX = makeRowVector(line1DiffX, line2DiffX);
        val diffY = makeRowVector(line1DiffY, line2DiffY);

        val div = determinant(diffX, diffY);
        if (div == 0)
        {
            return Optional.empty();
        }

        val line1MaxX = Math.max(line1StartX, line1EndX);
        val line1MaxY = Math.max(line1StartY, line1EndY);
        val line1MinX = Math.min(line1StartX, line1EndX);
        val line1MinY = Math.min(line1StartY, line1EndY);

        val line2MaxX = Math.max(line2StartX, line2EndX);
        val line2MaxY = Math.max(line2StartY, line2EndY);
        val line2MinX = Math.min(line2StartX, line2EndX);
        val line2MinY = Math.min(line2StartY, line2EndY);

        val line1Det = determinant(origin, intersectionPoint);
        val line2Det = determinant(current, next);
        val determinants = makeRowVector(line1Det, line2Det);

        val intersectionX = determinant(determinants, diffX) / div;
        val intersectionY = determinant(determinants, diffY) / div;

        if (intersectionX > line1MaxX || intersectionX < line1MinX || intersectionX > line2MaxX || intersectionX < line2MinX)
        {
            return Optional.empty();
        }

        if (intersectionY > line1MaxY || intersectionY < line1MinY || intersectionY > line2MaxY || intersectionY < line2MinY)
        {
            return Optional.empty();
        }

        val intersection = makeRowVector(intersectionX, intersectionY).transpose();
        return Optional.of(Intersection.builder()
                                       .vectorLength((float) distance(origin, intersection))
                                       .build());
    }

    private static double determinant(Access1D<Double> a, Access1D<Double> b)
    {
        return a.get(0) * b.get(1) - b.get(0) * a.get(1);
    }

    private static double distance(Access1D<Double> a, Access1D<Double> b)
    {
        return Math.sqrt(Math.pow(a.get(0) - b.get(0), 2) + Math.pow(a.get(1) - b.get(1), 2));
    }

    private static MatrixStore<Double> toColumnVector(MatrixStore<Double> input)
    {
        assert input.countRows() == 1 || input.countColumns() == 1 : "Parameter must be a vector (either row or column format)!";

        if (input.countColumns() == 1)
        {
            return input;
        }
        else
        {
            return input.transpose();
        }
    }

    public static int[] localMaximumIndices(double[] values)
    {
        // TODO: we currently assume that values[0] and values[l - 1] can *NOT* be local maxima (scipy argrelmaxima behavior), is this assumption true?

        var indices = new ArrayList<Integer>();
        for (var valueIndex = 1; valueIndex < values.length - 1; ++valueIndex)
        {
            val previous = values[valueIndex - 1];
            val current = values[valueIndex];
            val next = values[valueIndex + 1];

            if (current == next)
            {
                // we dont need to check 'next' again, because we already know its not a local maximum
                valueIndex++;
                continue;
            }

            if (current > previous && current > next)
            {
                indices.add(valueIndex);
                // we dont need to check 'next' again, because we already know its not a local maximum
                valueIndex++;
            }
        }

        return Ints.toArray(indices);
    }

    public static double scottsFactor(long numberOfRecords, long numberOfDimensions)
    {
        return Math.pow(numberOfRecords, -1.0d / (numberOfDimensions + 4.0d));
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