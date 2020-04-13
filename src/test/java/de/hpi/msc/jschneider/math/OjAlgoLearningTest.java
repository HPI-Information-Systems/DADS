package de.hpi.msc.jschneider.math;

import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;
import org.ojalgo.matrix.decomposition.QR;

import static org.assertj.core.api.Assertions.assertThat;

public class OjAlgoLearningTest extends TestCase
{
    public void testQRDecomposition()
    {
        val input = (new RowMatrixBuilder(3)
                             .append(new double[]{0.0d, 1.0d, 2.0d})
                             .append(new double[]{3.0d, 4.0d, 5.0d})
                             .append(new double[]{6.0d, 7.0d, 8.0d})
                             .append(new double[]{9.0d, -1.0d, -2.0d})
                             .build());

        val qr = QR.PRIMITIVE.make();
        assertThat(qr.compute(input)).isTrue();
        assertThat(qr.isComputed()).isTrue();

        assertThat(qr.getR().countRows()).isEqualTo(input.countColumns());
        assertThat(qr.getR().countColumns()).isEqualTo(input.countColumns());
    }

    public void testAccessOrder()
    {
        val matrix = (new RowMatrixBuilder(3)
                              .append(new double[]{0.0d, 1.0d, 2.0d})
                              .append(new double[]{3.0d, 4.0d, 5.0d})
                              .append(new double[]{6.0d, 7.0d, 8.0d})
                              .build());

        assertThat(matrix.get(0, 2)).isEqualTo(2.0d);
        assertThat(matrix.get(2, 0)).isEqualTo(6.0d);
    }
}
