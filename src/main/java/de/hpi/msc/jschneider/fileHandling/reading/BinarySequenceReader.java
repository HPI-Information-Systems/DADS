package de.hpi.msc.jschneider.fileHandling.reading;

import lombok.val;
import lombok.var;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinarySequenceReader implements SequenceReader
{
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
        maximumPosition = tryGetSize() / Float.BYTES - 1;
        minimumPosition = 0L;
    }

    private BinarySequenceReader(File file, long minimumPosition, long maximumPosition) throws FileNotFoundException
    {
        this.file = file;
        this.inputStream = new FileInputStream(file.getAbsolutePath());
        this.minimumPosition = minimumPosition;
        this.maximumPosition = maximumPosition;
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

    private Float tryReadNext()
    {
        try
        {
            val bytes = new byte[Float.BYTES];
            inputStream.read(bytes);
            return ByteBuffer.wrap(bytes).getFloat();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            tryClose();
            return 0.0f;
        }
    }

    @Override
    public long getSize()
    {
        return maximumPosition - minimumPosition + 1;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public float[] read(long start, int length)
    {
        val begin = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start));
        var end = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start + length - 1));
        return tryRead(begin, end);
    }

    private float[] tryRead(long begin, long end)
    {
        val length = end - begin + 1;
        val floats = new float[(int) length];
        try
        {
            inputStream.getChannel().position(begin * Float.BYTES);
            for (var i = 0; i < length; ++i)
            {
                floats[i] = tryReadNext();
            }
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            tryClose();
        }

        return floats;
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
