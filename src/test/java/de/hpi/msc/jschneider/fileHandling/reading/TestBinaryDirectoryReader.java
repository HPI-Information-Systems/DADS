package de.hpi.msc.jschneider.fileHandling.reading;

import junit.framework.TestCase;
import lombok.val;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinaryDirectoryReader extends TestCase
{
    private SequenceReader readerFromSequenceDirectory()
    {
        val path = getClass().getClassLoader().getResource("sequence-directory/").getFile();
        val directory = new File(path);
        return BinaryDirectoryReader.fromDirectory(directory);
    }

    public void testSize()
    {
        val reader = readerFromSequenceDirectory();

        assertThat(reader.getSize()).isEqualTo(8);
    }

    public void testReadSequence()
    {
        val reader = readerFromSequenceDirectory();

        assertThat(reader.read(0, 8)).containsExactly(0.0f, 1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f, 7.7f);
    }

    public void testReadPartly()
    {
        val reader = readerFromSequenceDirectory();

        assertThat(reader.read(0, 2)).containsExactly(0.0f, 1.1f);
        assertThat(reader.read(2, 3)).containsExactly(2.2f, 3.3f, 4.4f);
        assertThat(reader.read(5, 2)).containsExactly(5.5f, 6.6f);
        assertThat(reader.read(7, 1)).containsExactly(7.7f);
    }

    public void testSubReader()
    {
        val reader = readerFromSequenceDirectory();
        val subReader = reader.subReader(2, 4);

        assertThat(subReader.getSize()).isEqualTo(4);
        assertThat(subReader.read(0, 4)).containsExactly(2.2f, 3.3f, 4.4f, 5.5f);

        val subSubReader = subReader.subReader(2, 2);
        assertThat(subSubReader.getSize()).isEqualTo(2);
        assertThat(subSubReader.read(0, 2)).containsExactly(4.4f, 5.5f);
    }
}
