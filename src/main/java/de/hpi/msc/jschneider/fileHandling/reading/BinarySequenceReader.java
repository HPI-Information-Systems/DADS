package de.hpi.msc.jschneider.fileHandling.reading;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class BinarySequenceReader implements SequenceReader
{
    private static final Logger Log = LogManager.getLogger(BinarySequenceReader.class);

    public static SequenceReader fromFile(File file)
    {
        try
        {
            return new BinarySequenceReader(file, SystemParameters.getMaximumMessageSize());
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
    private final byte[] readBuffer;
    private int readBufferLength;

    private BinarySequenceReader(File file, int maximumMessageSize) throws NullPointerException, IllegalArgumentException, FileNotFoundException
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
        maximumPosition = tryGetSize() / Double.BYTES;
        minimumPosition = 0L;
        currentPosition = minimumPosition;
        readBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
    }

    private BinarySequenceReader(File file, long minimumPosition, long maximumPosition, int maximumMessageSize) throws FileNotFoundException
    {
        this.file = file;
        this.inputStream = new FileInputStream(file.getAbsolutePath());
        this.minimumPosition = minimumPosition;
        this.maximumPosition = maximumPosition;
        currentPosition = minimumPosition;
        readBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
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
    public long getSize()
    {
        return maximumPosition - minimumPosition;
    }

    @Override
    public long getPosition()
    {
        return currentPosition - minimumPosition;
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.DataTransferSynchronizationMessage.builder()
                                                                      .sender(sender)
                                                                      .receiver(receiver)
                                                                      .operationId(operationId)
                                                                      .numberOfElements(getSize())
                                                                      .bufferSize(readBuffer.length)
                                                                      .build();
    }

    @Override
    public boolean hasNext()
    {
        return currentPosition < maximumPosition;
    }

    @Override
    public void next()
    {
        if (!isOpen)
        {
            readBufferLength = 0;
            return;
        }

        try
        {
            inputStream.getChannel().position(currentPosition * Double.BYTES);
            readBufferLength = inputStream.read(readBuffer, 0, (int) Math.min(readBuffer.length, (maximumPosition - currentPosition) * Double.BYTES));
            currentPosition += readBufferLength / Double.BYTES;
        }
        catch (IOException ioException)
        {
            Log.error("Exception while reading from file!", ioException);
            readBufferLength = 0;
            tryClose();
        }
    }

    @Override
    public byte[] buffer()
    {
        return readBuffer;
    }

    @Override
    public int bufferLength()
    {
        return readBufferLength;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        try
        {
            val min = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start));
            val max = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start + length));

            return new BinarySequenceReader(file, min, max, readBuffer.length);
        }
        catch (FileNotFoundException fileNotFoundException)
        {
            fileNotFoundException.printStackTrace();
            return NullSequenceReader.get();
        }
    }
}
