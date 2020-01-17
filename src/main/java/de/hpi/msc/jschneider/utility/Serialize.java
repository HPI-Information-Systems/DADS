package de.hpi.msc.jschneider.utility;

import lombok.val;
import lombok.var;
import org.ojalgo.structure.Access1D;

import java.nio.ByteBuffer;

public class Serialize
{
    public static byte[] toBytes(float[] floats)
    {
        return toBytes(floats, 0, floats.length);
    }

    public static byte[] toBytes(float[] floats, int from, int to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= floats.length : "'To' out of range!";

        val range = to - from;
        val size = range * Float.BYTES;

        val bytes = new byte[size];
        for (int floatsIndex = from, bytesIndex = 0; floatsIndex < to; ++floatsIndex, bytesIndex += Float.BYTES)
        {
            insertBytes(bytes, bytesIndex, floats[floatsIndex]);
        }

        return bytes;
    }

    public static byte[] toBytes(Access1D<Double> access1D)
    {
        return toBytes(access1D, 0L, access1D.count());
    }

    public static byte[] toBytes(Access1D<Double> access1D, long from, long to)
    {
        val range = to - from;
        val size = range * Float.BYTES;
        assert size <= Integer.MAX_VALUE : "Unable to allocate more than Integer.MAX_VALUE bytes at once!";

        val bytes = new byte[(int) size];
        var bytesIndex = 0;
        for (var accessIndex = from; accessIndex < to; ++accessIndex)
        {
            insertBytes(bytes, bytesIndex, access1D.get(accessIndex).floatValue());
            bytesIndex += Float.BYTES;
        }

        return bytes;
    }

    private static void insertBytes(byte[] bytes, int bytesIndex, float value)
    {
        val convertedValue = ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
        System.arraycopy(convertedValue, 0, bytes, bytesIndex, convertedValue.length);
    }

    public static float[] toFloats(byte[] bytes)
    {
        return toFloats(bytes, 0, bytes.length);
    }

    public static float[] toFloats(byte[] bytes, int from, int to)
    {
        assert from >= 0 : "'From' out of range!";
        assert to >= from && to <= bytes.length : "'To' out of range";

        val range = to - from;
        assert range % Float.BYTES == 0 : "Range % 4 != 0";

        val size = range / Float.BYTES;

        val floats = new float[size];
        for (int floatsIndex = 0, bytesIndex = from; floatsIndex < floats.length; ++floatsIndex, bytesIndex += Float.BYTES)
        {
            insertFloat(floats, floatsIndex, bytes, bytesIndex);
        }

        return floats;
    }

    private static void insertFloat(float[] floats, int floatsIndex, byte[] bytes, int bytesIndex)
    {
        val convertedValue = ByteBuffer.wrap(bytes, bytesIndex, Float.BYTES).getFloat();
        floats[floatsIndex] = convertedValue;
    }
}
