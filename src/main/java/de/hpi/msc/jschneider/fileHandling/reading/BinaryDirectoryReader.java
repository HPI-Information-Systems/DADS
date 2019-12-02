package de.hpi.msc.jschneider.fileHandling.reading;

import lombok.val;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;

public class BinaryDirectoryReader implements SequenceReader
{
    public static SequenceReader fromDirectory(File directory)
    {
        try
        {
            return new BinaryDirectoryReader(directory);
        }
        catch (NullPointerException | IllegalArgumentException | FileNotFoundException exception)
        {
            exception.printStackTrace();
            return NullSequenceReader.get();
        }
    }

    private final SequenceReader[] sequenceReaders;
    private int currentSequenceReaderIndex = 0;
    private final long size;

    private BinaryDirectoryReader(File directory) throws NullPointerException, IllegalArgumentException, FileNotFoundException
    {
        if (directory == null)
        {
            throw new NullPointerException("Directory must not be null!");
        }

        if (!directory.exists() || !directory.isDirectory() || !directory.canRead())
        {
            throw new IllegalArgumentException(String.format("Unable to process the given directory at \"%1$s\"!", directory.getAbsolutePath()));
        }

        sequenceReaders = createSequenceReaders(directory);
        size = Arrays.stream(sequenceReaders).mapToLong(SequenceReader::getSize).sum();
    }

    private SequenceReader[] createSequenceReaders(File directory)
    {
        val files = directory.listFiles();
        if (files == null || files.length < 1)
        {
            return new SequenceReader[0];
        }

        val readers = new ArrayList<SequenceReader>();
        for (val filePath : Arrays.stream(files).map(File::getAbsolutePath).sorted().toArray(String[]::new))
        {
            val file = new File(filePath);
            val reader = BinarySequenceReader.fromFile(file);
            if (reader.isNull())
            {
                continue;
            }

            readers.add(reader);
        }

        return readers.toArray(new SequenceReader[0]);
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public boolean hasNext()
    {
        return currentReader().hasNext();
    }

    private SequenceReader currentReader()
    {
        if (currentSequenceReaderIndex >= sequenceReaders.length)
        {
            return NullSequenceReader.get();
        }

        return sequenceReaders[currentSequenceReaderIndex];
    }

    @Override
    public Float next()
    {
        val reader = currentReader();
        val next = reader.next();

        if (!reader.hasNext() && currentSequenceReaderIndex < sequenceReaders.length)
        {
            currentSequenceReaderIndex++;
        }

        return next;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }
}
