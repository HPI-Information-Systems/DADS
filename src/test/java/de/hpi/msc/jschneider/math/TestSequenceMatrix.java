package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSequenceMatrix extends TestCase
{
    public void testNotTransposed()
    {
        // sequence = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // sub-sequence-length = 5
        // convolution-size = 2
        val data = new double[]{1, 3, 5, 7, 9, 11, 13, 15};
        val matrix = new SequenceMatrix(3 /* sub-sequence-length - convolution-size */, data);

        assertThat(matrix.countColumns()).isEqualTo(3);
        assertThat(matrix.countRows()).isEqualTo(6);

        assertThat(matrix.get(0L, 0L)).isEqualTo(1.0d);
        assertThat(matrix.get(0L, 1L)).isEqualTo(3.0d);
        assertThat(matrix.get(0L, 2L)).isEqualTo(5.0d);

        assertThat(matrix.get(1L, 0L)).isEqualTo(3.0d);
        assertThat(matrix.get(1L, 1L)).isEqualTo(5.0d);
        assertThat(matrix.get(1L, 2L)).isEqualTo(7.0d);

        assertThat(matrix.get(2L, 0L)).isEqualTo(5.0d);
        assertThat(matrix.get(2L, 1L)).isEqualTo(7.0d);
        assertThat(matrix.get(2L, 2L)).isEqualTo(9.0d);

        assertThat(matrix.get(3L, 0L)).isEqualTo(7.0d);
        assertThat(matrix.get(3L, 1L)).isEqualTo(9.0d);
        assertThat(matrix.get(3L, 2L)).isEqualTo(11.0d);

        assertThat(matrix.get(4L, 0L)).isEqualTo(9.0d);
        assertThat(matrix.get(4L, 1L)).isEqualTo(11.0d);
        assertThat(matrix.get(4L, 2L)).isEqualTo(13.0d);

        assertThat(matrix.get(5L, 0L)).isEqualTo(11.0d);
        assertThat(matrix.get(5L, 1L)).isEqualTo(13.0d);
        assertThat(matrix.get(5L, 2L)).isEqualTo(15.0d);
    }

    public void testTransposed()
    {
        // sequence = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // sub-sequence-length = 5
        // convolution-size = 2
        val data = new double[]{1, 3, 5, 7, 9, 11, 13, 15};
        val matrix = new SequenceMatrix(3 /* sub-sequence-length - convolution-size */, data).transpose();

        assertThat(matrix.countColumns()).isEqualTo(6);
        assertThat(matrix.countRows()).isEqualTo(3);

        assertThat(matrix.get(0L, 0L)).isEqualTo(1.0d);
        assertThat(matrix.get(1L, 0L)).isEqualTo(3.0d);
        assertThat(matrix.get(2L, 0L)).isEqualTo(5.0d);

        assertThat(matrix.get(0L, 1L)).isEqualTo(3.0d);
        assertThat(matrix.get(1L, 1L)).isEqualTo(5.0d);
        assertThat(matrix.get(2L, 1L)).isEqualTo(7.0d);

        assertThat(matrix.get(0L, 2L)).isEqualTo(5.0d);
        assertThat(matrix.get(1L, 2L)).isEqualTo(7.0d);
        assertThat(matrix.get(2L, 2L)).isEqualTo(9.0d);

        assertThat(matrix.get(0L, 3L)).isEqualTo(7.0d);
        assertThat(matrix.get(1L, 3L)).isEqualTo(9.0d);
        assertThat(matrix.get(2L, 3L)).isEqualTo(11.0d);

        assertThat(matrix.get(0L, 4L)).isEqualTo(9.0d);
        assertThat(matrix.get(1L, 4L)).isEqualTo(11.0d);
        assertThat(matrix.get(2L, 4L)).isEqualTo(13.0d);

        assertThat(matrix.get(0L, 5L)).isEqualTo(11.0d);
        assertThat(matrix.get(1L, 5L)).isEqualTo(13.0d);
        assertThat(matrix.get(2L, 5L)).isEqualTo(15.0d);
    }

    public void testSubtractColumnBased()
    {
        // sequence = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // sub-sequence-length = 5
        // convolution-size = 2
        val data = new double[]{1, 3, 5, 7, 9, 11, 13, 15};
        val subtrahends = (new RowMatrixBuilder(3).append(new double[]{0.0d, 1.0d, 2.0d}).build());

        val matrix = new SequenceMatrix(3 /* sub-sequence-length - convolution-size */, data).subtractColumnBased(subtrahends);

        assertThat(matrix.get(0L, 0L)).isEqualTo(1.0d);
        assertThat(matrix.get(0L, 1L)).isEqualTo(2.0d);
        assertThat(matrix.get(0L, 2L)).isEqualTo(3.0d);

        assertThat(matrix.get(1L, 0L)).isEqualTo(3.0d);
        assertThat(matrix.get(1L, 1L)).isEqualTo(4.0d);
        assertThat(matrix.get(1L, 2L)).isEqualTo(5.0d);

        assertThat(matrix.get(2L, 0L)).isEqualTo(5.0d);
        assertThat(matrix.get(2L, 1L)).isEqualTo(6.0d);
        assertThat(matrix.get(2L, 2L)).isEqualTo(7.0d);

        assertThat(matrix.get(3L, 0L)).isEqualTo(7.0d);
        assertThat(matrix.get(3L, 1L)).isEqualTo(8.0d);
        assertThat(matrix.get(3L, 2L)).isEqualTo(9.0d);

        assertThat(matrix.get(4L, 0L)).isEqualTo(9.0d);
        assertThat(matrix.get(4L, 1L)).isEqualTo(10.0d);
        assertThat(matrix.get(4L, 2L)).isEqualTo(11.0d);

        assertThat(matrix.get(5L, 0L)).isEqualTo(11.0d);
        assertThat(matrix.get(5L, 1L)).isEqualTo(12.0d);
        assertThat(matrix.get(5L, 2L)).isEqualTo(13.0d);
    }

    public void testSubtractColumnBasedTransposed()
    {
        // sequence = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        // sub-sequence-length = 5
        // convolution-size = 2
        val data = new double[]{1, 3, 5, 7, 9, 11, 13, 15};
        val subtrahends = (new RowMatrixBuilder(3).append(new double[]{0.0d, 1.0d, 2.0d}).build());
        val matrix = new SequenceMatrix(3 /* sub-sequence-length - convolution-size */, data).subtractColumnBased(subtrahends).transpose();

        assertThat(matrix.countColumns()).isEqualTo(6);
        assertThat(matrix.countRows()).isEqualTo(3);

        assertThat(matrix.get(0L, 0L)).isEqualTo(1.0d);
        assertThat(matrix.get(1L, 0L)).isEqualTo(2.0d);
        assertThat(matrix.get(2L, 0L)).isEqualTo(3.0d);

        assertThat(matrix.get(0L, 1L)).isEqualTo(3.0d);
        assertThat(matrix.get(1L, 1L)).isEqualTo(4.0d);
        assertThat(matrix.get(2L, 1L)).isEqualTo(5.0d);

        assertThat(matrix.get(0L, 2L)).isEqualTo(5.0d);
        assertThat(matrix.get(1L, 2L)).isEqualTo(6.0d);
        assertThat(matrix.get(2L, 2L)).isEqualTo(7.0d);

        assertThat(matrix.get(0L, 3L)).isEqualTo(7.0d);
        assertThat(matrix.get(1L, 3L)).isEqualTo(8.0d);
        assertThat(matrix.get(2L, 3L)).isEqualTo(9.0d);

        assertThat(matrix.get(0L, 4L)).isEqualTo(9.0d);
        assertThat(matrix.get(1L, 4L)).isEqualTo(10.0d);
        assertThat(matrix.get(2L, 4L)).isEqualTo(11.0d);

        assertThat(matrix.get(0L, 5L)).isEqualTo(11.0d);
        assertThat(matrix.get(1L, 5L)).isEqualTo(12.0d);
        assertThat(matrix.get(2L, 5L)).isEqualTo(13.0d);
    }
}
