package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCalculate extends TestCase
{
    public void testColumnMeans()
    {
        val input = (new MatrixInitializer(3))
                .appendRow(new float[]{1.0f, 1.0f, 1.0f})
                .appendRow(new float[]{-1.0f, 0.0f, -1.0f})
                .appendRow(new float[]{1.0f, 2.0f, 2.0f})
                .appendRow(new float[]{-1.0f, 5.0f, 2.0f})
                .create();

        val means = Calculate.transposedColumnMeans(input);

        assertThat(means.countRows()).isEqualTo(1L);
        assertThat(means.countColumns()).isEqualTo(input.countColumns());

        assertThat(means.get(0, 0)).isEqualTo(0.0d);
        assertThat(means.get(0, 1)).isEqualTo(2.0d);
        assertThat(means.get(0, 2)).isEqualTo(1.0d);
    }

    public void testColumnCenteredDataMatrix()
    {
        val input = (new MatrixInitializer(3))
                .appendRow(new float[]{1.0f, 1.0f, 1.0f})
                .appendRow(new float[]{-1.0f, 0.0f, -1.0f})
                .appendRow(new float[]{1.0f, 2.0f, 2.0f})
                .appendRow(new float[]{-1.0f, 5.0f, 2.0f})
                .create();

        val dataMatrix = Calculate.columnCenteredDataMatrix(input);

        assertThat(dataMatrix.countRows()).isEqualTo(input.countRows());
        assertThat(dataMatrix.countColumns()).isEqualTo(input.countColumns());

        assertThat(dataMatrix.get(0, 0)).isEqualTo(1.0d);
        assertThat(dataMatrix.get(0, 1)).isEqualTo(-1.0d);
        assertThat(dataMatrix.get(0, 2)).isEqualTo(0.0d);

        assertThat(dataMatrix.get(1, 0)).isEqualTo(-1.0d);
        assertThat(dataMatrix.get(1, 1)).isEqualTo(-2.0d);
        assertThat(dataMatrix.get(1, 2)).isEqualTo(-2.0d);

        assertThat(dataMatrix.get(2, 0)).isEqualTo(1.0d);
        assertThat(dataMatrix.get(2, 1)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(2, 2)).isEqualTo(1.0d);

        assertThat(dataMatrix.get(3, 0)).isEqualTo(-1.0d);
        assertThat(dataMatrix.get(3, 1)).isEqualTo(3.0d);
        assertThat(dataMatrix.get(3, 2)).isEqualTo(1.0d);
    }

    public void testLog2()
    {
        assertThat(Calculate.log2(8.0d)).isEqualTo(3.0d);
        assertThat(Calculate.log2(7.0d)).isBetween(2.0d, 3.0d);
    }

    public void testNextPowerOfTwo()
    {
        assertThat(Calculate.nextPowerOfTwo(8)).isEqualTo(8);
        assertThat(Calculate.nextPowerOfTwo(7)).isEqualTo(8);
    }
}
