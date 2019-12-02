package de.hpi.msc.jschneider.fileHandling.writing;

import de.hpi.msc.jschneider.fileHandling.reading.BinarySequenceReader;
import junit.framework.TestCase;
import lombok.val;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinarySequenceWriter extends TestCase
{
    private final List<File> files = new ArrayList<>();

    private File createFile()
    {
        val resourceFolder = getClass().getClassLoader().getResource(".").getFile();
        val file = Paths.get(resourceFolder, String.format("%1$s.bin", UUID.randomUUID())).toFile();
        files.add(file);

        return file;
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        for (val file : files)
        {
            file.delete();
        }
    }

    private void expectSequence(File file, Float[] sequence)
    {
        val reader = BinarySequenceReader.fromFile(file);

        assertThat(reader.getSize()).isEqualTo(sequence.length);
        for (val record : sequence)
        {
            assertThat(reader.next()).isEqualTo(record);
        }
    }

    public void testWriteSimpleSequence()
    {
        val file = createFile();
        val writer = BinarySequenceWriter.fromFile(file);

        val sequence = new Float[]{-1.1f, 0.9f, 1.0f, 4.25f};

        writer.write(sequence);

        expectSequence(file, sequence);
    }
}
