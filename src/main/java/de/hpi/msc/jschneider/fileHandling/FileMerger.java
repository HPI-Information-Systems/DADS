package de.hpi.msc.jschneider.fileHandling;

import lombok.val;
import lombok.var;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class FileMerger
{
    private final FileOutputStream outputStream;
    private final Iterator<File> inputFileIterator;

    public FileMerger(File outputFile, Iterator<File> inputFiles) throws Exception
    {
        inputFileIterator = inputFiles;

        if (outputFile == null)
        {
            throw new NullPointerException("File must not be null!");
        }

        outputFile.delete();

        if (!outputFile.exists())
        {
            if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs())
            {
                throw new Exception(String.format("Unable to create directory of %1$s!", outputFile.getAbsolutePath()));
            }

            if (!outputFile.createNewFile())
            {
                throw new Exception(String.format("Unable to create file %1$s!", outputFile.getAbsolutePath()));
            }
        }

        if (!outputFile.isFile())
        {
            throw new IllegalArgumentException(String.format("%1$s is not a file!", outputFile.getAbsolutePath()));
        }

        if (!outputFile.canWrite())
        {
            throw new IllegalArgumentException(String.format("Unable to write to %1$s!", outputFile.getAbsolutePath()));
        }

        outputStream = new FileOutputStream(outputFile.getAbsolutePath(), false);
    }

    public void merge() throws IOException
    {
        val readBuffer = new byte[0xffff];
        var readBufferLength = 0;

        while (inputFileIterator.hasNext())
        {
            val inputFile = inputFileIterator.next();
            val fileInputStream = new FileInputStream(inputFile.getAbsolutePath());

            while (true)
            {
                readBufferLength = fileInputStream.read(readBuffer, 0, readBuffer.length);

                if (readBufferLength < 1)
                {
                    break;
                }

                outputStream.write(readBuffer, 0, readBufferLength);
            }
            outputStream.write('\n');

            fileInputStream.close();
            inputFile.delete();
        }

        outputStream.close();
    }
}
