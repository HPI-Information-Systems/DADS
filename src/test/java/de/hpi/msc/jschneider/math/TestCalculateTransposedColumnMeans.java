package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCalculateTransposedColumnMeans extends TestCase
{
    public void testColumnMeans()
    {
        val input = (new MatrixInitializer(3))
                .appendRow(new float[]{1.0f, 1.0f, 1.0f})
                .appendRow(new float[]{-1.0f, -1.0f, -1.0f})
                .appendRow(new float[]{1.0f, 2.0f, 3.0f})
                .create();

        val means = Calculate.transposedColumnMeans(input);

        assertThat(means.countRows()).isEqualTo(3L);
        assertThat(means.countColumns()).isOne();

        assertThat(means.get(0, 0)).isEqualTo(1.0d);
        assertThat(means.get(1, 0)).isEqualTo(-1.0d);
        assertThat(means.get(2, 0)).isEqualTo(2.0d);
    }
}
