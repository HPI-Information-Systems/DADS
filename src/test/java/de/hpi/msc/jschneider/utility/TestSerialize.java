package de.hpi.msc.jschneider.utility;

import com.google.common.primitives.Bytes;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
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
                              .append(new double[]{0.0d, 1.0d, 2.0d})
                              .append(new double[]{3.0d, 4.0d, 5.0d})
                              .append(new double[]{6.0d, 7.0d, 8.0d})
                              .build());
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

    public void testSerializeDoublesInPlace()
    {
        val doubles = new DoubleArrayList(10);
        for (var i = 0; i < 10; ++i)
        {
            doubles.add(i);
        }

        val sendBuffer = new byte[Double.BYTES * 4];
        val converter = ByteBuffer.wrap(sendBuffer);
        val it = doubles.iterator();

        assertThat(Serialize.inPlace(it, sendBuffer)).isEqualTo(4 * Double.BYTES);
        assertThat(converter.getDouble(0 * Double.BYTES)).isEqualTo(0.0d);
        assertThat(converter.getDouble(1 * Double.BYTES)).isEqualTo(1.0d);
        assertThat(converter.getDouble(2 * Double.BYTES)).isEqualTo(2.0d);
        assertThat(converter.getDouble(3 * Double.BYTES)).isEqualTo(3.0d);

        assertThat(Serialize.inPlace(it, sendBuffer)).isEqualTo(4 * Double.BYTES);
        assertThat(converter.getDouble(0 * Double.BYTES)).isEqualTo(4.0d);
        assertThat(converter.getDouble(1 * Double.BYTES)).isEqualTo(5.0d);
        assertThat(converter.getDouble(2 * Double.BYTES)).isEqualTo(6.0d);
        assertThat(converter.getDouble(3 * Double.BYTES)).isEqualTo(7.0d);

        assertThat(Serialize.inPlace(it, sendBuffer)).isEqualTo(2 * Double.BYTES);
        assertThat(converter.getDouble(0 * Double.BYTES)).isEqualTo(8.0d);
        assertThat(converter.getDouble(1 * Double.BYTES)).isEqualTo(9.0d);
        assertThat(converter.getDouble(2 * Double.BYTES)).isEqualTo(6.0d);
        assertThat(converter.getDouble(3 * Double.BYTES)).isEqualTo(7.0d);

        assertThat(it.hasNext()).isFalse();
    }

    public void testSerializeDoublesBackInPlace()
    {
        val sendDoubles = new DoubleArrayList(10);
        for (var i = 0; i < 10; ++i)
        {
            sendDoubles.add(i);
        }

        val it = sendDoubles.iterator();
        val buffer = new byte[Double.BYTES * 4];

        val receiveDoubles = new DoubleBigArrayBigList(10);

        var bufferLength = Serialize.inPlace(it, buffer);
        Serialize.backInPlace(buffer, bufferLength, receiveDoubles);
        assertThat(receiveDoubles.getDouble(0)).isEqualTo(sendDoubles.getDouble(0));
        assertThat(receiveDoubles.getDouble(1)).isEqualTo(sendDoubles.getDouble(1));
        assertThat(receiveDoubles.getDouble(2)).isEqualTo(sendDoubles.getDouble(2));
        assertThat(receiveDoubles.getDouble(3)).isEqualTo(sendDoubles.getDouble(3));

        bufferLength = Serialize.inPlace(it, buffer);
        Serialize.backInPlace(buffer, bufferLength, receiveDoubles);
        assertThat(receiveDoubles.getDouble(4)).isEqualTo(sendDoubles.getDouble(4));
        assertThat(receiveDoubles.getDouble(5)).isEqualTo(sendDoubles.getDouble(5));
        assertThat(receiveDoubles.getDouble(6)).isEqualTo(sendDoubles.getDouble(6));
        assertThat(receiveDoubles.getDouble(7)).isEqualTo(sendDoubles.getDouble(7));

        bufferLength = Serialize.inPlace(it, buffer);
        Serialize.backInPlace(buffer, bufferLength, receiveDoubles);
        assertThat(receiveDoubles.getDouble(8)).isEqualTo(sendDoubles.getDouble(8));
        assertThat(receiveDoubles.getDouble(9)).isEqualTo(sendDoubles.getDouble(9));

        assertThat(it.hasNext()).isFalse();
    }
}
