package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;
import lombok.var;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMatrixIterator extends TestCase
{
    public void testNext()
    {
        val matrix = (new RowMatrixBuilder(3L)
                              .append(new double[]{0.0d, 1.0d, 2.0d})
                              .append(new double[]{3.0d, 4.0d, 5.0d})
                              .append(new double[]{6.0d, 7.0d, 8.0d})
                              .build());

        val it = new MatrixIterator(matrix);
        for (var i = 0; i < matrix.count(); ++i)
        {
            assertThat(it.hasNext()).isTrue();
            assertThat(it.nextDouble()).isEqualTo((double) i);
        }

        assertThat(it.hasNext()).isFalse();
    }
}
