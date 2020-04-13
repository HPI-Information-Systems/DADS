package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRowMatrixBuilder extends TestCase
{
    public void testInitialize3x3()
    {
        val matrix = (new RowMatrixBuilder(3))
                .append(new double[]{1.0d, 2.0d, 3.0d})
                .append(new double[]{4.0d, 5.0d, 6.0d})
                .append(new double[]{7.0d, 8.0d, 9.0d})
                .build();

        assertThat(matrix.countRows()).isEqualTo(3);
        assertThat(matrix.countColumns()).isEqualTo(3);
    }

    public void testConcat()
    {
        val firstMatrix = (new RowMatrixBuilder(3))
                .append(new double[]{0.0d, 1.0d, 2.0d})
                .append(new double[]{3.0d, 4.0d, 5.0d})
                .build();

        val secondMatrix = (new RowMatrixBuilder(3))
                .append(new double[]{6.0d, 7.0d, 8.0d})
                .build();

        val result = RowMatrixBuilder.concat(firstMatrix, secondMatrix);

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
