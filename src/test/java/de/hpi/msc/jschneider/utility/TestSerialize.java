package de.hpi.msc.jschneider.utility;

import com.google.common.primitives.Bytes;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import junit.framework.TestCase;
import lombok.val;
import lombok.var;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSerialize extends TestCase
{
    public void testDoublesToBytes()
    {
        val doubles = new double[]{0.0d, -1.0d, 1.0d, Double.MIN_VALUE, Double.MAX_VALUE};
        val expectedBytes = new ArrayList<byte[]>();

        for (val value : doubles)
        {
            expectedBytes.add(ByteBuffer.allocate(Double.BYTES).putDouble(value).array());
        }

        assertThat(Serialize.toBytes(doubles)).containsExactly(Bytes.concat(expectedBytes.toArray(new byte[0][0])));
    }

    public void testAccess1DToBytes()
    {
        val matrix = (new RowMatrixBuilder(3L)
                              .appendRow(new double[]{0.0d, 1.0d, 2.0d})
                              .appendRow(new double[]{3.0d, 4.0d, 5.0d})
                              .appendRow(new double[]{6.0d, 7.0d, 8.0d})
                              .create());
        val expectedBytes = new ArrayList<byte[]>();

        for (var i = 0L; i < matrix.count(); ++i)
        {
            expectedBytes.add(ByteBuffer.allocate(Double.BYTES).putDouble(matrix.get(i)).array());
        }

        assertThat(Serialize.toBytes(matrix)).containsExactly(Bytes.concat(expectedBytes.toArray(new byte[0][0])));
    }

    public void testBytesToDoubles()
    {
        val expectedDoubles = new double[]{0.0d, -1.0d, 1.0d, Double.MIN_VALUE, Double.MAX_VALUE};
        val bytes = new ArrayList<byte[]>();

        for (val value : expectedDoubles)
        {
            bytes.add(ByteBuffer.allocate(Double.BYTES).putDouble(value).array());
        }

        assertThat(Serialize.toDoubles(Bytes.concat(bytes.toArray(new byte[0][0])))).containsExactly(expectedDoubles);
    }
}
