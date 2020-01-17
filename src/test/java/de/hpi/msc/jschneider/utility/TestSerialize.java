package de.hpi.msc.jschneider.utility;

import com.google.common.primitives.Bytes;
import junit.framework.TestCase;
import lombok.val;
import lombok.var;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSerialize extends TestCase
{
    public void testFloatsToBytes()
    {
        val floats = new float[]{0.0f, -1.0f, 1.0f, Float.MIN_VALUE, Float.MAX_VALUE};
        val expectedBytes = new ArrayList<byte[]>();

        for (val value : floats)
        {
            expectedBytes.add(ByteBuffer.allocate(Float.BYTES).putFloat(value).array());
        }

        assertThat(Serialize.toBytes(floats)).containsExactly(Bytes.concat(expectedBytes.toArray(new byte[0][0])));
    }

    public void testAccess1DToBytes()
    {
        val matrix = (new MatrixInitializer(3L)
                              .appendRow(new float[]{0.0f, 1.0f, 2.0f})
                              .appendRow(new float[]{3.0f, 4.0f, 5.0f})
                              .appendRow(new float[]{6.0f, 7.0f, 8.0f})
                              .create());
        val expectedBytes = new ArrayList<byte[]>();

        for (var i = 0L; i < matrix.count(); ++i)
        {
            expectedBytes.add(ByteBuffer.allocate(Float.BYTES).putFloat(matrix.get(i).floatValue()).array());
        }

        assertThat(Serialize.toBytes(matrix)).containsExactly(Bytes.concat(expectedBytes.toArray(new byte[0][0])));
    }

    public void testBytesToFloats()
    {
        val expectedFloats = new float[]{0.0f, -1.0f, 1.0f, Float.MIN_VALUE, Float.MAX_VALUE};
        val bytes = new ArrayList<byte[]>();

        for (val value : expectedFloats)
        {
            bytes.add(ByteBuffer.allocate(Float.BYTES).putFloat(value).array());
        }

        assertThat(Serialize.toFloats(Bytes.concat(bytes.toArray(new byte[0][0])))).containsExactly(expectedFloats);
    }
}
