package de.hpi.msc.jschneider.math;

import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.Primitive64Matrix;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.structure.Access1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import static org.ojalgo.function.constant.PrimitiveMath.SUBTRACT;

public class Calculate
{
    private static final Logger Log = LogManager.getLogger(Calculate.class);

    public static final double FLOATING_POINT_TOLERANCE = 0.00001d;

    private static final double TWO_PI = 2 * Math.PI;

    private static final MatrixStore<Double> ORIGIN_2D = makeRowVector(0.0d, 0.0d);

    public static boolean isSame(Number a, Number b)
    {
        return Math.abs(a.doubleValue() - b.doubleValue()) <= FLOATING_POINT_TOLERANCE;
    }

    public static boolean isMore(Number a, Number b)
    {
        return a.doubleValue() - b.doubleValue() > FLOATING_POINT_TOLERANCE;
    }

    public static boolean isLess(Number a, Number b)
    {
        return b.doubleValue() - a.doubleValue() > FLOATING_POINT_TOLERANCE;
    }

    public static MatrixStore<Double> makeFilledRowVector(long length, double value)
    {
        val matrix = Primitive64Matrix.FACTORY.rows(DoubleStream.generate(() -> value).limit(length).toArray());
        return MatrixStore.PRIMITIVE64.makeWrapper(matrix).get();
    }

    public static MatrixStore<Double> makeRowVector(double... values)
    {
        val matrix = Primitive64Matrix.FACTORY.rows(values);
        return MatrixStore.PRIMITIVE64.makeWrapper(matrix).get();
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

    public static MatrixStore<Double> rotation(MatrixStore<Double> referenceVector, MatrixStore<Double> unitVector)
    {
        val vec1 = toColumnVector(referenceVector).multiply(1.0d / Math.sqrt(referenceVector.aggregateAll(Aggregator.SUM2)));
        val vec2 = toColumnVector(unitVector).multiply(1.0d / Math.sqrt(unitVector.aggregateAll(Aggregator.SUM2)));

        assert vec1.countRows() == 3 : "The rotation can only be performed on a 3d vector!";
        assert vec2.countRows() == 3 : "The rotation can only be performed on a 3d vector!";

        val cross = cross(vec1, vec2);
        val crossLength = Math.sqrt(cross.aggregateAll(Aggregator.SUM2));
        val dot = vec1.dot(vec2);
        val identity = MatrixStore.PRIMITIVE64.makeIdentity(3).get();
        val k = (new RowMatrixBuilder(3))
                .append(new double[]{0.0f, -cross.get(2), cross.get(1)})
                .append(new double[]{cross.get(2), 0.0f, -cross.get(0)})
                .append(new double[]{-cross.get(1), cross.get(0), 0.0f})
                .build();

        return identity.add(k).add(k.multiply(k.multiply((1 - dot) / (crossLength * crossLength))));
    }

    private static MatrixStore<Double> cross(MatrixStore<Double> a, MatrixStore<Double> b)
    {
        return (new RowMatrixBuilder(1))
                .append(new double[]{a.get(1) * b.get(2) - a.get(2) * b.get(1)})
                .append(new double[]{a.get(2) * b.get(0) - a.get(0) * b.get(2)})
                .append(new double[]{a.get(0) * b.get(1) - a.get(1) * b.get(0)})
                .build();
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
        if (input instanceof SequenceMatrix)
        {
            return ((SequenceMatrix) input).subtractColumnBased(transposedColumnMeans);
        }

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

    public static IntersectionCollection[] intersections(MatrixStore<Double> reducedProjection, List<MatrixStore<Double>> intersectionPoints, long firstSubSequenceIndex)
    {
        assert reducedProjection.countRows() == 2 : "ReducedProjection must have 2 dimensions in column vector format!";
        assert reducedProjection.countColumns() > 1 : "ReducedProjection must have at least 2 records!";

        val numberOfSegments = intersectionPoints.size();
        val intersectionCollections = new IntersectionCollection[numberOfSegments];
        val estimatedNumberOfIntersectionsPerSegment = (long) Math.ceil(reducedProjection.countColumns() * 0.25d);
        for (var i = 0; i < intersectionCollections.length; ++i)
        {
            intersectionCollections[i] = new IntersectionCollection(i, estimatedNumberOfIntersectionsPerSegment);
        }

        val nextIntersectionCreationIndex = new Counter(firstSubSequenceIndex * numberOfSegments);
        for (var columnIndex = 0; columnIndex < reducedProjection.countColumns() - 1; ++columnIndex)
        {
            val current = reducedProjection.sliceColumn(columnIndex);
            val next = reducedProjection.sliceColumn(columnIndex + 1);

            for (val intersectionSegment : intersectionSegmentsToCheck(current, next, numberOfSegments))
            {
                val intersection = tryCalculateIntersection(intersectionPoints.get(intersectionSegment),
                                                            firstSubSequenceIndex + columnIndex,
                                                            nextIntersectionCreationIndex.getAndIncrement(),
                                                            current,
                                                            next);
                if (!intersection.isPresent())
                {
                    continue;
                }

                intersectionCollections[intersectionSegment].getIntersections().add(intersection.get());
            }
        }

        Log.info("Estimated number of intersections: {}; Actual number of intersections (avg.) {}.",
                 estimatedNumberOfIntersectionsPerSegment,
                 Arrays.stream(intersectionCollections).mapToLong(collection -> collection.getIntersections().size64()).sum() / (double) numberOfSegments);

        return intersectionCollections;
    }

    public static List<MatrixStore<Double>> makeIntersectionPoints(double radiusLength, int numberOfSegments)
    {
        val intersectionPoints = new ArrayList<MatrixStore<Double>>(numberOfSegments);
        val angleStep = TWO_PI / numberOfSegments;

        for (var i = 0; i < numberOfSegments; ++i)
        {
            val x = Math.cos(angleStep * i) * radiusLength;
            val y = Math.sin(angleStep * i) * radiusLength;
            intersectionPoints.add(makeRowVector(x, y).transpose());
        }

        return intersectionPoints;
    }

    private static int[] intersectionSegmentsToCheck(Access1D<Double> current, Access1D<Double> next, int numberOfSegments)
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

        val currentIntersectionPointIndex = (int) (currentTheta / TWO_PI * numberOfSegments);
        val nextIntersectionPointIndex = (int) (nextTheta / TWO_PI * numberOfSegments);
        var diff = Math.abs(currentIntersectionPointIndex - nextIntersectionPointIndex);
        val halfSamples = numberOfSegments / 2;
        if (diff > halfSamples)
        {
            if (nextIntersectionPointIndex > halfSamples)
            {
                diff = Math.abs(currentIntersectionPointIndex + numberOfSegments - nextIntersectionPointIndex);
            }
            else if (currentIntersectionPointIndex > halfSamples)
            {
                diff = Math.abs(currentIntersectionPointIndex - numberOfSegments - nextIntersectionPointIndex);
            }
        }
        diff = Math.min(diff, halfSamples);
        val intersectionPointIndices = new int[2 * (diff + 1)];
        val intersectionPointOffset = -diff - 1;
        for (var i = 0; i < intersectionPointIndices.length; ++i)
        {
            intersectionPointIndices[i] = Math.floorMod(currentIntersectionPointIndex + intersectionPointOffset + i, numberOfSegments);
        }

        return intersectionPointIndices;
    }

    private static Optional<Intersection> tryCalculateIntersection(Access1D<Double> intersectionPoint,
                                                                   long subSequenceIndex,
                                                                   long intersectionCreationIndex,
                                                                   Access1D<Double> current,
                                                                   Access1D<Double> next)
    {
        val line1StartX = ORIGIN_2D.get(0);
        val line1StartY = ORIGIN_2D.get(1);
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

        val line1Det = determinant(ORIGIN_2D, intersectionPoint);
        val line2Det = determinant(current, next);
        val determinants = makeRowVector(line1Det, line2Det);

        val intersectionX = determinant(determinants, diffX) / div;
        val intersectionY = determinant(determinants, diffY) / div;

//        if (isMore(intersectionX, line1MaxX) || isLess(intersectionX, line1MinX) || isMore(intersectionX, line2MaxX) || isLess(intersectionX, line2MinX))
//        {
//            return Optional.empty();
//        }
//
//        if (isMore(intersectionY, line1MaxY) || isLess(intersectionY, line1MinY) || isMore(intersectionY, line2MaxY) || isLess(intersectionY, line2MinY))
//        {
//            return Optional.empty();
//        }

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
                                       .intersectionDistance(length2D(intersection))
                                       .subSequenceIndex(subSequenceIndex)
                                       .creationIndex(intersectionCreationIndex)
                                       .build());
    }

    private static double determinant(Access1D<Double> a, Access1D<Double> b)
    {
        return a.get(0) * b.get(1) - b.get(0) * a.get(1);
    }

    private static double length2D(Access1D<Double> point)
    {
        return Math.sqrt(point.get(0) * point.get(0) + point.get(1) * point.get(1));
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

    public static int minimumDistanceIndexSorted(double value, double[] possibleResults)
    {
        var closestIndex = 0;
        var closestDistance = Double.MAX_VALUE;
        for (var index = 0; index < possibleResults.length; ++index)
        {
            val distance = Math.abs(possibleResults[index] - value);

            if (distance >= closestDistance)
            {
                break;
            }

            closestIndex = index;
            closestDistance = distance;
        }

        return closestIndex;
    }

    public static double scottsFactor(long numberOfRecords, long numberOfDimensions)
    {
        return Math.pow(numberOfRecords, -1.0d / (numberOfDimensions + 4.0d));
    }

    public static Map<Integer, Long> nodeDegrees(Collection<GraphEdge> edges)
    {
        val incomingEdges = new HashMap<Integer, Counter>();
        val outgoingEdges = new HashMap<Integer, Counter>();
        val nodeHashes = edges.stream()
                              .flatMap(edge -> Arrays.stream(new Integer[]{edge.getFrom().hashCode(), edge.getTo().hashCode()}))
                              .collect(Collectors.toSet());
        for (var nodeHash : nodeHashes)
        {
            val hash = nodeHash.hashCode();
            incomingEdges.put(hash, new Counter(0L));
            outgoingEdges.put(hash, new Counter(0L));
        }

        for (val edge : edges)
        {
            val nodeFromHash = edge.getFrom().hashCode();
            val nodeToHash = edge.getTo().hashCode();

            outgoingEdges.get(nodeFromHash).increment();
            incomingEdges.get(nodeToHash).increment();
        }

        return nodeHashes.stream().collect(Collectors.toMap(hash -> hash,
                                                            hash -> incomingEdges.get(hash).get() + outgoingEdges.get(hash).get()));
    }

    public static MatrixStore<Double> subtractColumnBased(MatrixStore<Double> matrix, MatrixStore<Double> columnSubtrahends)
    {
        assert columnSubtrahends.countRows() == 1L
               && columnSubtrahends.countColumns() == matrix.countColumns() : "ColumnSubtrahends have wrong format";

        return matrix.operateOnColumns(SUBTRACT, columnSubtrahends).get();
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

    public static int nextSmallerMultipleOf(int actualAmount, int baseNumber)
    {
        return actualAmount - (actualAmount % baseNumber);
    }
}
