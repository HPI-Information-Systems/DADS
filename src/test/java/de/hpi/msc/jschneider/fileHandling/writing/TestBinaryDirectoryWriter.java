package de.hpi.msc.jschneider.fileHandling.writing;

import de.hpi.msc.jschneider.fileHandling.reading.BinaryDirectoryReader;
import junit.framework.TestCase;
import lombok.val;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinaryDirectoryWriter extends TestCase
{
    private final List<File> directories = new ArrayList<>();

    private File createDirectory()
    {
        val resourceFolder = getClass().getClassLoader().getResource(".").getFile();
        val file = Paths.get(resourceFolder, String.format("%1$s/", UUID.randomUUID())).toFile();
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

    private void expectSequences(File directory, Float[]... sequences)
    {
        val reader = BinaryDirectoryReader.fromDirectory(directory);

        for (val sequence : sequences)
        {
            for (val record : sequence)
            {
                assertThat(reader.next()).isEqualTo(record);
            }
        }
    }

    public void testWriteSequences()
    {
        val directory = createDirectory();
        val writer = BinaryDirectoryWriter.fromDirectory(directory);

        val firstSequence = new Float[]{0.0f, 1.1f, 2.2f, 3.3f};
        val secondSequence = new Float[]{4.4f, 5.5f, 6.6f, 7.7f};

        writer.write(firstSequence);
        writer.write(secondSequence);

        expectSequences(directory, firstSequence, secondSequence);
    }
}
