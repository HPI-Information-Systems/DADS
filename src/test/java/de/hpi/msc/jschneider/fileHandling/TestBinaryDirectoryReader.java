package de.hpi.msc.jschneider.fileHandling;

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
        assertThat(reader.hasNext()).isTrue();
    }

    public void testReadSequence()
    {
        val reader = readerFromSequenceDirectory();

        assertThat(reader.next()).isEqualTo(0.0f);
        assertThat(reader.next()).isEqualTo(1.1f);
        assertThat(reader.next()).isEqualTo(2.2f);
        assertThat(reader.next()).isEqualTo(3.3f);
        assertThat(reader.next()).isEqualTo(4.4f);
        assertThat(reader.next()).isEqualTo(5.5f);
        assertThat(reader.next()).isEqualTo(6.6f);
        assertThat(reader.next()).isEqualTo(7.7f);
        assertThat(reader.hasNext()).isFalse();
    }
}
