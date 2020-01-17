package de.hpi.msc.jschneider.fileHandling.reading;

import de.hpi.msc.jschneider.utility.Serialize;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class BinaryDirectoryReader implements SequenceReader
{
    private static final Logger Log = LogManager.getLogger(BinaryDirectoryReader.class);

    private final SortedSet<SequenceReaderWrapper> sequenceReaders = new TreeSet<>((first, second) -> (int) (first.startIndex - second.startIndex));

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

    private long currentPosition;
    private final long minimumPosition;
    private final long maximumPosition;

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

        createSequenceReaders(directory);
        minimumPosition = 0L;
        maximumPosition = sequenceReaders.stream().mapToLong(reader -> reader.getSequenceReader().getSize()).sum() - 1;
        currentPosition = minimumPosition;
    }

    private BinaryDirectoryReader(Collection<SequenceReaderWrapper> sequenceReaders, long minimumPosition, long maximumPosition)
    {
        this.sequenceReaders.addAll(sequenceReaders);
        this.minimumPosition = minimumPosition;
        this.maximumPosition = maximumPosition;
        currentPosition = minimumPosition;
    }

    private void createSequenceReaders(File directory)
    {
        val files = directory.listFiles();
        if (files == null || files.length < 1)
        {
            return;
        }

        var currentStartIndex = 0L;
        for (val filePath : Arrays.stream(files).map(File::getAbsolutePath).sorted().toArray(String[]::new))
        {
            val file = new File(filePath);
            val reader = BinarySequenceReader.fromFile(file);
            if (reader.isNull())
            {
                continue;
            }

            val wrapper = SequenceReaderWrapper.builder()
                                               .sequenceReader(reader)
                                               .startIndex(currentStartIndex)
                                               .endIndex(currentStartIndex + reader.getSize() - 1)
                                               .build();
            sequenceReaders.add(wrapper);
            currentStartIndex += reader.getSize();
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
        return Float.BYTES;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        val values = read(currentPosition, (long) Math.floor(maximumPartSize / (double) elementSizeInBytes()));
        currentPosition += values.length;
        return Serialize.toBytes(values);
    }

    @Override
    public float[] read(long start, long length)
    {
        val begin = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start));
        val end = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start + length - 1));
        var actualLength = end - begin + 1;
        if (actualLength > Integer.MAX_VALUE)
        {
            Log.error("Unable to allocate more than Integer.MAX_VALUE floats at once!");
            actualLength = Integer.MAX_VALUE;
        }

        val floats = FloatBuffer.allocate((int) actualLength);
        var first = true;
        for (val wrapper : readers(begin, end))
        {
            var beginRead = 0L;
            if (first)
            {
                beginRead = begin - wrapper.getStartIndex();
                first = false;
            }

            floats.put(wrapper.getSequenceReader().read(beginRead, length - floats.position()));
        }

        return floats.array();
    }

    private Collection<? extends SequenceReaderWrapper> readers(long begin, long end)
    {
        return sequenceReaders.stream()
                              .filter(wrapper -> wrapper.getStartIndex() <= end && wrapper.getEndIndex() >= begin)
                              .collect(Collectors.toList());
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        val min = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start));
        val max = Math.max(minimumPosition, Math.min(maximumPosition, minimumPosition + start + length - 1));

        return new BinaryDirectoryReader(createSubReaderWrappers(min, max), min, max);
    }

    private Collection<SequenceReaderWrapper> createSubReaderWrappers(long minimumPosition, long maximumPosition)
    {
        val newWrappers = new ArrayList<SequenceReaderWrapper>();
        for (val existingWrapper : readers(minimumPosition, maximumPosition))
        {
            val newWrapperStartIndex = Math.max(existingWrapper.getStartIndex(), minimumPosition);
            val newWrapperEndIndex = Math.min(existingWrapper.getEndIndex(), maximumPosition);

            val newReader = existingWrapper.getSequenceReader().subReader(newWrapperStartIndex - existingWrapper.getStartIndex(), newWrapperEndIndex - newWrapperStartIndex + 1);
            newWrappers.add(SequenceReaderWrapper.builder()
                                                 .startIndex(newWrapperStartIndex)
                                                 .endIndex(newWrapperEndIndex)
                                                 .sequenceReader(newReader)
                                                 .build());
        }

        return newWrappers;
    }

    @Builder
    private static class SequenceReaderWrapper
    {
        @NonNull @Getter
        private SequenceReader sequenceReader;
        @NonNull @Getter
        private long startIndex;
        @NonNull @Getter
        private long endIndex;
    }
}
