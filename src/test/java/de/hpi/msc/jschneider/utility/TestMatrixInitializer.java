package de.hpi.msc.jschneider.utility;

import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

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
}
