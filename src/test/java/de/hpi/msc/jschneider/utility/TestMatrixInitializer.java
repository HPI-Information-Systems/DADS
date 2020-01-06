package de.hpi.msc.jschneider.utility;

import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

public class TestMatrixInitializer extends TestCase
{
    public void testInitialize3x3()
    {
        val matrix = (new MatrixInitializer(3))
                .appendRow(new float[]{1.0f, 2.0f, 3.0f})
                .appendRow(new float[]{4.0f, 5.0f, 6.0f})
                .appendRow(new float[]{7.0f, 8.0f, 9.0f})
                .create();

        assertThat(matrix.countRows()).isEqualTo(3);
        assertThat(matrix.countColumns()).isEqualTo(3);
    }

    public void testConcat()
    {
        val firstMatrix = (new MatrixInitializer(3))
                .appendRow(new float[]{0.0f, 1.0f, 2.0f})
                .appendRow(new float[]{3.0f, 4.0f, 5.0f})
                .create();

        val secondMatrix = (new MatrixInitializer(3))
                .appendRow(new float[]{6.0f, 7.0f, 8.0f})
                .create();

        val result = MatrixInitializer.concat(firstMatrix, secondMatrix);

        assertThat(result.countColumns()).isEqualTo(3);
        assertThat(result.countRows()).isEqualTo(3);

        assertThat(result.get(0, 0)).isEqualTo(0.0d);
        assertThat(result.get(0, 1)).isEqualTo(1.0d);
        assertThat(result.get(0, 2)).isEqualTo(2.0d);
        assertThat(result.get(1, 0)).isEqualTo(3.0d);
        assertThat(result.get(1, 1)).isEqualTo(4.0d);
        assertThat(result.get(1, 2)).isEqualTo(5.0d);
        assertThat(result.get(2, 0)).isEqualTo(6.0d);
        assertThat(result.get(2, 1)).isEqualTo(7.0d);
        assertThat(result.get(2, 2)).isEqualTo(8.0d);
    }
}
