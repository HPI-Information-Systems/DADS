package de.hpi.msc.jschneider.fileHandling.writing;

import junit.framework.TestCase;
import lombok.val;
import lombok.var;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinaryDirectoryWriter extends TestCase
{
    private final List<File> directories = new ArrayList<>();

    private File createDirectory() throws URISyntaxException
    {
        val resourceFolder = getClass().getClassLoader().getResource(".").toURI();
        val file = Paths.get(Paths.get(resourceFolder).toString(), String.format("%1$s", UUID.randomUUID())).toFile();
        directories.add(file);

        return file;
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        for (val directory : directories)
        {
            directory.delete();
        }
    }

    private void expectSequences(File directory, double[]... sequences)
    {
        val reader = BinaryDirectoryReader.fromDirectory(directory);
        var start = 0;
        for (val sequence : sequences)
        {
            assertThat(reader.read(start, sequence.length)).containsExactly(sequence);
            start += sequence.length;
        }
    }

    public void testWriteSequences() throws URISyntaxException
    {
        val directory = createDirectory();
        val writer = BinaryDirectoryWriter.fromDirectory(directory);

        val firstSequence = new double[]{0.0d, 1.1d, 2.2d, 3.3d};
        val secondSequence = new double[]{4.4d, 5.5d, 6.6d, 7.7d};

        writer.write(firstSequence);
        writer.write(secondSequence);

        expectSequences(directory, firstSequence, secondSequence);
    }
}
