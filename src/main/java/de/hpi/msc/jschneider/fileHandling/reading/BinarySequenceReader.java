package de.hpi.msc.jschneider.fileHandling.reading;

import lombok.val;

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

    private final FileInputStream inputStream;
    private final long size;
    private boolean isOpen = true;

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

        inputStream = new FileInputStream(file.getAbsolutePath());
        size = tryGetSize() / Float.BYTES;
    }

    @Override
    public boolean hasNext()
    {
        if (!isOpen)
        {
            return false;
        }

        return tryGetPosition() <= tryGetSize() - Float.BYTES;
    }

    private long tryGetPosition()
    {
        if (!isOpen)
        {
            return 0;
        }

        try
        {
            return inputStream.getChannel().position();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            tryClose();
            return 0;
        }
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

    @Override
    public Float next()
    {
        if (!hasNext())
        {
            tryClose();
            return 0.0f;
        }

        val next = tryReadNext();

        if (!hasNext())
        {
            tryClose();
        }

        return next;
    }

    private Float tryReadNext()
    {
        try
        {
            val bytes = new byte[Float.BYTES];
            inputStream.read(bytes);
            return ByteBuffer.wrap(bytes).getFloat();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            tryClose();
            return 0.0f;
        }
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }
}
