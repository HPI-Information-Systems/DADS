package de.hpi.msc.jschneider.fileHandling.writing;

import de.hpi.msc.jschneider.fileHandling.reading.BinarySequenceReader;
import junit.framework.TestCase;
import lombok.val;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinarySequenceWriter extends TestCase
{
    private final List<File> files = new ArrayList<>();

    private File createFile() throws URISyntaxException
    {
        val resourceFolder = getClass().getClassLoader().getResource(".").toURI();
        val file = Paths.get(Paths.get(resourceFolder).toString(), String.format("%1$s.bin", UUID.randomUUID())).toFile();
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

    private void expectSequence(File file, float[] sequence)
    {
        val reader = BinarySequenceReader.fromFile(file);

        assertThat(reader.getSize()).isEqualTo(sequence.length);
        assertThat(reader.read(0, sequence.length)).containsExactly(sequence);
    }

    public void testWriteSimpleSequence() throws URISyntaxException
    {
        val file = createFile();
        val writer = BinarySequenceWriter.fromFile(file);

        val sequence = new float[]{-1.1f, 0.9f, 1.0f, 4.25f};

        writer.write(sequence);

        expectSequence(file, sequence);
    }
}
