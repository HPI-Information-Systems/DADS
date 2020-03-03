//package de.hpi.msc.jschneider.math;
//
//import de.hpi.msc.jschneider.utility.MatrixInitializer;
//import junit.framework.TestCase;
//import lombok.val;
//import lombok.var;
//
//import java.util.Arrays;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//public class TestCalculate extends TestCase
//{
//    public void testIsSame()
//    {
//        assertThat(Calculate.isSame(0.0d, -0.0d)).isTrue();
//        assertThat(Calculate.isSame(0.0d, Calculate.FLOATING_POINT_TOLERANCE)).isTrue();
//        assertThat(Calculate.isSame(0.0d, -Calculate.FLOATING_POINT_TOLERANCE)).isTrue();
//        assertThat(Calculate.isSame(Calculate.FLOATING_POINT_TOLERANCE, 0.0d)).isTrue();
//        assertThat(Calculate.isSame(-Calculate.FLOATING_POINT_TOLERANCE, 0.0d)).isTrue();
//
//        assertThat(Calculate.isSame(0.0d, 1.0d)).isFalse();
//        assertThat(Calculate.isSame(1.0d, 0.0d)).isFalse();
//    }
//
//    public void testIsMore()
//    {
//        assertThat(Calculate.isMore(1.0d, 0.0d)).isTrue();
//        assertThat(Calculate.isMore(0.0d, -0.0d)).isFalse();
//        assertThat(Calculate.isMore(Calculate.FLOATING_POINT_TOLERANCE, 0.0d)).isFalse();
//    }
//
//    public void testIsLess()
//    {
//        assertThat(Calculate.isLess(0.0d, 1.0d)).isTrue();
//        assertThat(Calculate.isLess(-0.0d, 0.0d)).isFalse();
//        assertThat(Calculate.isLess(0.0d, Calculate.FLOATING_POINT_TOLERANCE)).isFalse();
//    }
//
//    public void testMakeRange()
//    {
//        assertThat(Calculate.makeRange(0.0d, 5.0d, 5)).containsExactly(0.0d, 1.0d, 2.0d, 3.0d, 4.0d);
//        assertThat(Calculate.makeRange(1.0d, 5.0d, 4)).containsExactly(1.0d, 2.0d, 3.0d, 4.0d);
//        assertThat(Calculate.makeRange(0.0d, 10.0d, 1)).containsExactly(0.0d);
//    }
//
//    public void testColumnMeans()
//    {
//        val input = (new MatrixInitializer(3))
//                .appendRow(new double[]{1.0d, 1.0d, 1.0d})
//                .appendRow(new double[]{-1.0d, 0.0d, -1.0d})
//                .appendRow(new double[]{1.0d, 2.0d, 2.0d})
//                .appendRow(new double[]{-1.0d, 5.0d, 2.0d})
//                .create();
//
//        val means = Calculate.transposedColumnMeans(input);
//
//        assertThat(means.countRows()).isEqualTo(1L);
//        assertThat(means.countColumns()).isEqualTo(input.countColumns());
//
//        assertThat(means.get(0, 0)).isEqualTo(0.0d);
//        assertThat(means.get(0, 1)).isEqualTo(2.0d);
//        assertThat(means.get(0, 2)).isEqualTo(1.0d);
//    }
//
//    public void testColumnCenteredDataMatrix()
//    {
//        val input = (new MatrixInitializer(3))
//                .appendRow(new double[]{1.0d, 1.0d, 1.0d})
//                .appendRow(new double[]{-1.0d, 0.0d, -1.0d})
//                .appendRow(new double[]{1.0d, 2.0d, 2.0d})
//                .appendRow(new double[]{-1.0d, 5.0d, 2.0d})
//                .create();
//
//        val dataMatrix = Calculate.columnCenteredDataMatrix(input);
//
//        assertThat(dataMatrix.countRows()).isEqualTo(input.countRows());
//        assertThat(dataMatrix.countColumns()).isEqualTo(input.countColumns());
//
//        assertThat(dataMatrix.get(0, 0)).isEqualTo(1.0d);
//        assertThat(dataMatrix.get(0, 1)).isEqualTo(-1.0d);
//        assertThat(dataMatrix.get(0, 2)).isEqualTo(0.0d);
//
//        assertThat(dataMatrix.get(1, 0)).isEqualTo(-1.0d);
//        assertThat(dataMatrix.get(1, 1)).isEqualTo(-2.0d);
//        assertThat(dataMatrix.get(1, 2)).isEqualTo(-2.0d);
//
//        assertThat(dataMatrix.get(2, 0)).isEqualTo(1.0d);
//        assertThat(dataMatrix.get(2, 1)).isEqualTo(0.0d);
//        assertThat(dataMatrix.get(2, 2)).isEqualTo(1.0d);
//
//        assertThat(dataMatrix.get(3, 0)).isEqualTo(-1.0d);
//        assertThat(dataMatrix.get(3, 1)).isEqualTo(3.0d);
//        assertThat(dataMatrix.get(3, 2)).isEqualTo(1.0d);
//    }
//
//    public void testAngleBetween()
//    {
//        val unitX = Calculate.makeRowVector(1.0d, 0.0d, 0.0d);
//        val unitY = Calculate.makeRowVector(0.0d, 1.0d, 0.0d);
//        val unitZ = Calculate.makeRowVector(0.0d, 0.0d, 1.0d);
//        val negativeUnitX = Calculate.makeRowVector(-1.0d, 0.0d, 0.0d);
//
//        assertThat(Calculate.angleBetween(unitX, unitY)).isEqualTo(Math.PI * 0.5d);
//        assertThat(Calculate.angleBetween(unitX, unitZ)).isEqualTo(Math.PI * 0.5d);
//        assertThat(Calculate.angleBetween(unitX, negativeUnitX)).isEqualTo(Math.PI);
//    }
//
//    public void testIntersections()
//    {
//        val reducedProjection = (new MatrixInitializer(2L)
//                                         .appendRow(new double[]{1.0d, 0.0d})
//                                         .appendRow(new double[]{0.0d, 1.0d})
//                                         .appendRow(new double[]{-1.0d, 0.0d})
//                                         .appendRow(new double[]{0.0d, -1.0d})
//                                         .create()
//                                         .transpose());
//        val numberOfSegments = 4;
//
//        val intersections = Calculate.intersections(reducedProjection, 0L, numberOfSegments);
//
//        assertThat(intersections.length).isEqualTo(numberOfSegments);
//        assertThat(Arrays.stream(intersections)
//                         .allMatch(collection -> collection.getIntersections()
//                                                           .stream()
//                                                           .allMatch(intersection -> Calculate.isSame(intersection.getIntersectionDistance(), 1.0d))))
//                .isTrue();
//
//        for (var intersectionSegment = 0; intersectionSegment < numberOfSegments; ++intersectionSegment)
//        {
//            assertThat(intersections[intersectionSegment].getIntersectionSegment()).isEqualTo(intersectionSegment);
//        }
//    }
//
//    public void testIntersections2()
//    {
//        val reducedProjection = (new MatrixInitializer(2L)
//                                         .appendRow(new double[]{0.5d, 0.0d})
//                                         .appendRow(new double[]{1.0d, 0.0d})
//                                         .create()
//                                         .transpose());
//        val numberOfSegments = 4;
//        val intersections = Calculate.intersections(reducedProjection, 0L, numberOfSegments);
//
//        assertThat(intersections.length).isEqualTo(numberOfSegments);
//        assertThat(Arrays.stream(intersections).allMatch(collection -> collection.getIntersections().isEmpty())).isTrue();
//    }
//
//    public void testLocalMaximumIndices()
//    {
//        val values = new double[]{5.0d, 1.0d, 2.0d, 0.5d, 4.0d, 3.0d, 9.0d};
//        val indices = Calculate.localMaximumIndices(values);
//
//        assertThat(indices).containsExactly(2, 4);
//    }
//
//    public void testLog2()
//    {
//        assertThat(Calculate.log2(8.0d)).isEqualTo(3.0d);
//        assertThat(Calculate.log2(7.0d)).isBetween(2.0d, 3.0d);
//    }
//
//    public void testNextPowerOfTwo()
//    {
//        assertThat(Calculate.nextPowerOfTwo(8)).isEqualTo(8);
//        assertThat(Calculate.nextPowerOfTwo(7)).isEqualTo(8);
//    }
//}
