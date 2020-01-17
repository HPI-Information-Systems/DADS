package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.Serialize;
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
        val expectedMatrix = (new MatrixInitializer(3L)
                                      .appendRow(new float[]{0.0f, 1.0f, 2.0f})
                                      .appendRow(new float[]{3.0f, 4.0f, 5.0f})
                                      .appendRow(new float[]{6.0f, 7.0f, 8.0f})
                                      .appendRow(new float[]{9.0f, 10.0f, 11.0f})
                                      .create());

        val values = new float[]{
                0.0f, 3.0f, 6.0f, 9.0f,
                1.0f, 4.0f, 7.0f, 10.0f,
                2.0f, 5.0f, 8.0f, 11.0f
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
