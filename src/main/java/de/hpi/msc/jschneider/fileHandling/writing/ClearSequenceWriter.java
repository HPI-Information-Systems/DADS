package de.hpi.msc.jschneider.fileHandling.writing;

import de.hpi.msc.jschneider.utility.Serialize;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import lombok.val;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class ClearSequenceWriter implements SequenceWriter
{
    private final FileOutputStream outputStream;
    private final Writer outputWriter;
    private boolean isOpen = true;

    private ClearSequenceWriter(File file) throws NullPointerException, IllegalArgumentException, Exception
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

    public static SequenceWriter fromFile(File file)
    {
        try
        {
            return new ClearSequenceWriter(file);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return NullSequenceWriter.get();
        }
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void write(DoubleBigList records)
    {
        if (!isOpen)
        {
            return;
        }

        try
        {
            for (val record : records)
            {
                outputWriter.write(record + "\n");
            }
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            close();
        }
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
