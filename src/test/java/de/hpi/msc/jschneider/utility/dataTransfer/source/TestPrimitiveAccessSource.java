package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveAccessSource extends TestCase
{
    public void testRead()
    {
        val matrix = (new RowMatrixBuilder(3L).appendRow(new double[]{0.0d, 4.0d, 8.0d})
                                              .appendRow(new double[]{1.0d, 5.0d, 9.0d})
                                              .appendRow(new double[]{2.0d, 6.0d, 10.0d})
                                              .appendRow(new double[]{3.0d, 7.0d, 11.0d})
                                              .create());
        val source = GenericDataSource.create(matrix);

        assertThat(source.isAtEnd()).isFalse();

        assertThat(Serialize.toDoubles(source.read(5 * Double.BYTES))).containsExactly(0.0d, 1.0d, 2.0d, 3.0d, 4.0d);
        assertThat(Serialize.toDoubles(source.read(Double.BYTES + 1))).containsExactly(5.0d);
        assertThat(Serialize.toDoubles(source.read(4 * Double.BYTES))).containsExactly(6.0d, 7.0d, 8.0d, 9.0d);
        assertThat(Serialize.toDoubles(source.read(1 * Double.BYTES))).containsExactly(10.0d);
        assertThat(Serialize.toDoubles(source.read(5 * Double.BYTES))).containsExactly(11.0d);
        assertThat(Serialize.toDoubles(source.read(5 * Double.BYTES))).isEmpty();

        assertThat(source.isAtEnd()).isTrue();
    }
}
