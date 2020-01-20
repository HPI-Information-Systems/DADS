package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.Serialize;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveAccessSource extends TestCase
{
    public void testRead()
    {
        val matrix = (new MatrixInitializer(3L).appendRow(new float[]{0.0f, 4.0f, 8.0f})
                                               .appendRow(new float[]{1.0f, 5.0f, 9.0f})
                                               .appendRow(new float[]{2.0f, 6.0f, 10.0f})
                                               .appendRow(new float[]{3.0f, 7.0f, 11.0f})
                                               .create());
        val source = GenericDataSource.create(matrix);

        assertThat(source.isAtEnd()).isFalse();

        assertThat(Serialize.toFloats(source.read(5 * Float.BYTES))).containsExactly(0.0f, 1.0f, 2.0f, 3.0f, 4.0f);
        assertThat(Serialize.toFloats(source.read(5))).containsExactly(5.0f);
        assertThat(Serialize.toFloats(source.read(4 * Float.BYTES))).containsExactly(6.0f, 7.0f, 8.0f, 9.0f);
        assertThat(Serialize.toFloats(source.read(1 * Float.BYTES))).containsExactly(10.0f);
        assertThat(Serialize.toFloats(source.read(5 * Float.BYTES))).containsExactly(11.0f);
        assertThat(Serialize.toFloats(source.read(5 * Float.BYTES))).isEmpty();

        assertThat(source.isAtEnd()).isTrue();
    }
}
