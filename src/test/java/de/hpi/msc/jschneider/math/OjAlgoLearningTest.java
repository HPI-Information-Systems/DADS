package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import junit.framework.TestCase;
import lombok.val;
import org.ojalgo.matrix.decomposition.QR;

import static org.assertj.core.api.Assertions.assertThat;

public class OjAlgoLearningTest extends TestCase
{
    public void testQRDecomposition()
    {
        val input = (new MatrixInitializer(3)
                             .appendRow(new float[]{0.0f, 1.0f, 2.0f})
                             .appendRow(new float[]{3.0f, 4.0f, 5.0f})
                             .appendRow(new float[]{6.0f, 7.0f, 8.0f})
                             .appendRow(new float[]{9.0f, -1.0f, -2.0f})
                             .create());

        val qr = QR.PRIMITIVE.make();
        assertThat(qr.compute(input)).isTrue();
        assertThat(qr.isComputed()).isTrue();

        assertThat(qr.getR().countRows()).isEqualTo(input.countColumns());
        assertThat(qr.getR().countColumns()).isEqualTo(input.countColumns());
    }

    public void testAccessOrder()
    {
        val matrix = (new MatrixInitializer(3)
                              .appendRow(new float[]{0.0f, 1.0f, 2.0f})
                              .appendRow(new float[]{3.0f, 4.0f, 5.0f})
                              .appendRow(new float[]{6.0f, 7.0f, 8.0f})
                              .create());

        assertThat(matrix.get(0, 2)).isEqualTo(2.0d);
        assertThat(matrix.get(2, 0)).isEqualTo(6.0d);
    }
}
