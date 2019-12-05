package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCalculateColumnCenteredDataMatrix extends TestCase
{
    public void testColumnCenteredDataMatrix()
    {
        val input = (new MatrixInitializer(3))
                .appendRow(new float[]{1.0f, 1.0f, 1.0f})
                .appendRow(new float[]{-1.0f, -1.0f, -1.0f})
                .appendRow(new float[]{1.0f, 2.0f, 3.0f})
                .create();

        val dataMatrix = Calculate.columnCenteredDataMatrix(input);

        assertThat(dataMatrix.countRows()).isEqualTo(3L);
        assertThat(dataMatrix.countColumns()).isEqualTo(3L);

        assertThat(dataMatrix.get(0, 0)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(0, 1)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(0, 2)).isEqualTo(0.0d);

        assertThat(dataMatrix.get(1, 0)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(1, 1)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(1, 2)).isEqualTo(0.0d);

        assertThat(dataMatrix.get(2, 0)).isEqualTo(-1.0d);
        assertThat(dataMatrix.get(2, 1)).isEqualTo(0.0d);
        assertThat(dataMatrix.get(2, 2)).isEqualTo(1.0d);
    }
}
