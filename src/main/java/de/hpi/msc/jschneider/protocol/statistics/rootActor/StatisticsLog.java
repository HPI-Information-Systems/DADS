package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsProtocol;
import lombok.val;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.LocalDateTime;

public class StatisticsLog
{
    private FileOutputStream outputStream;
    private Writer outputWriter;
    private boolean isOpen = true;

    public StatisticsLog(File file)
    {
        try
        {
            outputStream = createOpenOutputStream(file);
            outputWriter = new PrintWriter(outputStream);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            isOpen = false;
            outputWriter = null;
        }

    }

    private FileOutputStream createOpenOutputStream(File file) throws Exception
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

        return new FileOutputStream(file.getAbsolutePath(), false);
    }

    public void log(StatisticsEvents.StatisticsEvent event)
    {
        tryWrite(event.toString());
    }

    private void tryWrite(String message)
    {
        if (!isOpen)
        {
            return;
        }

        try
        {
            val currentTime = LocalDateTime.now().format(StatisticsProtocol.DATE_FORMAT);
            val stringBuilder = new StringBuilder().append("[")
                                                   .append(currentTime)
                                                   .append("]  -  ")
                                                   .append(message)
                                                   .append("\n");

            outputWriter.write(stringBuilder.toString());
            outputWriter.flush();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
            close();
        }
    }

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
            outputWriter.close();
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
    }
}
