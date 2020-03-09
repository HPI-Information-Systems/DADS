package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;
import org.ojalgo.type.context.NumberContext;

import java.math.MathContext;
import java.math.RoundingMode;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveMatrixSink extends TestCase
{
    public void testClose()
    {
        val expectedMatrix = (new RowMatrixBuilder(3L)
                                      .appendRow(new double[]{0.0d, 1.0d, 2.0d})
                                      .appendRow(new double[]{3.0d, 4.0d, 5.0d})
                                      .appendRow(new double[]{6.0d, 7.0d, 8.0d})
                                      .appendRow(new double[]{9.0d, 10.0d, 11.0d})
                                      .create());

        val values = new double[]{
                0.0d, 3.0d, 6.0d, 9.0d,
                1.0d, 4.0d, 7.0d, 10.0d,
                2.0d, 5.0d, 8.0d, 11.0d
        };

        val sink = new PrimitiveMatrixSink();

        sink.write(Serialize.toBytes(values, 0, 4));
        sink.write(Serialize.toBytes(values, 4, 6));
        sink.write(Serialize.toBytes(values, 6, 11));
        sink.write(Serialize.toBytes(values, 11, 12));
        sink.close();

        val actualMatrix = sink.getMatrix(expectedMatrix.countColumns());

        assertThat(actualMatrix.equals(expectedMatrix, NumberContext.getMath(new MathContext(6, RoundingMode.HALF_UP)))).isTrue();

    }
}
