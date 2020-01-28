package de.hpi.msc.jschneider.fileHandling.reading;

import junit.framework.TestCase;
import lombok.val;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBinarySequenceReader extends TestCase
{
    private SequenceReader readerFromSimpleSequence()
    {
        val path = getClass().getClassLoader().getResource("simple-sequence.bin").getFile();
        val file = new File(path);
        return BinarySequenceReader.fromFile(file);
    }

    public void testSize()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.getSize()).isEqualTo(4);
    }

    public void testReadSimpleSequence()
    {
        val reader = readerFromSimpleSequence();

        assertThat(reader.read(0, 4)).containsExactly(-1.1d, 0.9d, 1.0d, 4.25d);
    }

    public void testReadPartly()
    {
        val reader = readerFromSimpleSequence();
        assertThat(reader.read(0, 2)).containsExactly(-1.1d, 0.9d);
        assertThat(reader.read(2, 2)).containsExactly(1.0d, 4.25d);
    }

    public void testSubReader()
    {
        val reader = readerFromSimpleSequence();
        val subReader = reader.subReader(2, 2);

        assertThat(subReader.getSize()).isEqualTo(2);
        assertThat(subReader.read(0, 2)).containsExactly(1.0d, 4.25d);

        val subSubReader = subReader.subReader(1, 1);
        assertThat(subSubReader.getSize()).isEqualTo(1);
        assertThat(subSubReader.read(0, 1)).containsExactly(4.25d);
    }
}
