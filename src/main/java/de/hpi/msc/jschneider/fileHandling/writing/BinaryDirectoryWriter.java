package de.hpi.msc.jschneider.fileHandling.writing;

import lombok.val;

import java.io.File;
import java.nio.file.Paths;

public class BinaryDirectoryWriter implements SequenceWriter
{
    private final File directory;
    private int currentSliceIndex = 0;

    private BinaryDirectoryWriter(File directory) throws Exception
    {
        if (directory == null)
        {
            throw new NullPointerException("Directory must not be null!");
        }

        if (!directory.exists() && !directory.mkdirs())
        {
            throw new Exception(String.format("Unable to create %1$s!", directory.getAbsolutePath()));
        }

        if (!directory.canWrite())
        {
            throw new Exception(String.format("Unable to write to %1$s!", directory.getAbsolutePath()));
        }

        this.directory = directory;
    }

    public static SequenceWriter fromDirectory(File directory)
    {
        try
        {
            return new BinaryDirectoryWriter(directory);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
            return NullSequenceWriter.get();
        }
    }

    @Override
    public void write(float[] records)
    {
        val file = Paths.get(directory.getAbsolutePath(), String.format("%06d.bin", currentSliceIndex)).toFile();
        val writer = BinarySequenceWriter.fromFile(file);
        writer.write(records);
        writer.close();

        currentSliceIndex++;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void close()
    {
    }
}
