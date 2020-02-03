package de.hpi.msc.jschneider.fileHandling.reading;

import de.hpi.msc.jschneider.utility.Serialize;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinarySequenceReader implements SequenceReader
{
    private static final Logger Log = LogManager.getLogger(BinarySequenceReader.class);

    public static SequenceReader fromFile(File file)
    {
        try
        {
            return new BinarySequenceReader(file);
        }
        catch (NullPointerException | IllegalArgumentException | FileNotFoundException exception)
        {
            exception.printStackTrace();
            return NullSequenceReader.get();
        }
    }

    private final File file;
    private final FileInputStream inputStream;
    private boolean isOpen = true;
    private long currentPosition;
    private final long minimumPosition;
    private final long maximumPosition;

    private BinarySequenceReader(File file) throws NullPointerException, IllegalArgumentException, FileNotFoundException
    {
        if (file == null)
        {
            throw new NullPointerException("File must not be null!");
        }

        if (!file.exists() || !file.isFile() || !file.canRead())
        {
            throw new IllegalArgumentException(String.format("Unable to process the given file \"%1$s\"!", file.getAbsolutePath()));
        }

        this.file = file;
        inputStream = new FileInputStream(file.getAbsolutePath());
        maximumPosition = tryGetSize() / elementSizeInBytes() - 1;
        minimumPosition = 0L;
        currentPosition = minimumPosition;
    }

    private BinarySequenceReader(File file, long minimumPosition, long maximumPosition) throws FileNotFoundException
    {
        this.file = file;
        this.inputStream = new FileInputStream(file.getAbsolutePath());
        this.minimumPosition = minimumPosition;
        this.maximumPosition = maximumPosition;
        currentPosition = minimumPosition;
    }

    private long tryGetSize()
    {
        if (!isOpen)
        {
            return 0;
        }

        try
        {
            return inputStream.getChannel().size();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            tryClose();
            return 0;
        }
    }

    private void tryClose()
    {
        if (!isOpen)
        {
            return;
        }

        isOpen = false;

        try
        {
            inputStream.close();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
    }

    private double tryReadNext()
    {
        try
        {
            val bytes = new byte[elementSizeInBytes()];
            inputStream.read(bytes);
            return ByteBuffer.wrap(bytes).getDouble();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            tryClose();
            return 0.0d;
        }
    }

    @Override
    public long getSize()
    {
        return maximumPosition - minimumPosition + 1;
    }

    @Override
    public long getPosition()
    {
        return currentPosition - minimumPosition;
    }

    @Override
    public boolean isAtEnd()
    {
        return currentPosition >= maximumPosition;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public int elementSizeInBytes()
    {
        return Double.BYTES;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        val values = read(currentPosition, (long) Math.floor(maximumPartSize / (double) elementSizeInBytes()));
        currentPosition += values.length;

        return Serialize.toBytes(values);
    }

    @Override
    public double[] read(long start, long length)
    {
        val begin = Math.max(minimumPosition, Math.min(maximumPosition, start));
        var end = Math.max(minimumPosition, Math.min(maximumPosition, begin + length - 1));
        return tryRead(begin, end);
    }

    private double[] tryRead(long begin, long end)
    {
        var length = end - begin + 1;
        if (length > Integer.MAX_VALUE)
        {
            Log.error("Can not allocate more than Integer.MAX_VALUE doubles at once!");
            length = Integer.MAX_VALUE;
        }

        val doubles = new double[(int) length];
        try
        {
            inputStream.getChannel().position(begin * elementSizeInBytes());
            for (var i = 0; i < length; ++i)
            {
                doubles[i] = tryReadNext();
            }
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            tryClose();
        }

        return doubles;
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        try
        {
            val min = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start));
            val max = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start + length - 1));

            return new BinarySequenceReader(file, min, max);
        }
        catch (FileNotFoundException fileNotFoundException)
        {
            fileNotFoundException.printStackTrace();
            return NullSequenceReader.get();
        }
    }
}
