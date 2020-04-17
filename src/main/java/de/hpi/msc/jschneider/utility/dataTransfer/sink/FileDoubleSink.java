package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class FileDoubleSink implements DataSink
{
    private double[] receiveBuffer;
    private int receiveBufferLength;
    private final FileOutputStream outputStream;
    private final Writer outputWriter;
    private boolean isOpen = true;
    private boolean isFirstValue = true;

    public FileDoubleSink(File file) throws Exception
    {
        if (file == null)
        {
            throw new NullPointerException("File must not be null!");
        }

        file.delete();

        if (!file.exists())
        {
            if (!file.getParentFile().exists() && !file.getParentFile().mkdirs())
            {
                throw new Exception(String.format("Unable to create directory of %1$s!", file.getAbsolutePath()));
            }

            if (!file.createNewFile())
            {
                throw new Exception(String.format("Unable to create file %1$s!", file.getAbsolutePath()));
            }
        }

        if (!file.isFile())
        {
            throw new IllegalArgumentException(String.format("%1$s is not a file!", file.getAbsolutePath()));
        }

        if (!file.canWrite())
        {
            throw new IllegalArgumentException(String.format("Unable to write to %1$s!", file.getAbsolutePath()));
        }

        outputStream = new FileOutputStream(file.getAbsolutePath(), false);
        outputWriter = new PrintWriter(outputStream);
    }

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        assert message.getBufferSize() % Double.BYTES == 0 : "BufferSize % ElementSize != 0!";

        receiveBuffer = new double[message.getBufferSize() / Double.BYTES];
        receiveBufferLength = 0;
    }

    @SneakyThrows
    @Override
    public void write(byte[] part, int partLength)
    {
        val numberOfDoubles = Serialize.backInPlace(part, partLength, receiveBuffer);

        for (var doublesIndex = 0; doublesIndex < numberOfDoubles; ++doublesIndex)
        {
            if (isFirstValue)
            {
                isFirstValue = false;
                outputWriter.write(String.valueOf(receiveBuffer[doublesIndex]));
                continue;
            }

            outputWriter.write("\n" + receiveBuffer[doublesIndex]);
        }
        outputWriter.flush();
    }

    @Override
    public void close()
    {
        tryClose();
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
            outputWriter.flush();
            outputStream.close();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
    }
}
