import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucar.unidata.sruth.Misc;

public class FileTruncateTest {
    private static final String TEST_PATH = "/tmp/FileTruncateTest";
    private static final int    SIZE      = 2048;
    private static final Random random    = new Random();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        removeTestPath();
    }

    @After
    public void tearDown() throws Exception {
    }

    private static void removeTestPath() throws IOException,
            InterruptedException {
        final int status = Misc.system("rm", "-f", TEST_PATH);
        assertEquals(0, status);
    }

    @Test
    public void testTruncate() throws IOException, InterruptedException {
        final Path path = Paths.get(TEST_PATH);
        final byte[] bytes = new byte[SIZE];
        random.nextBytes(bytes);
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        removeTestPath();
        SeekableByteChannel channel = Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        channel.write(buf);
        channel.close();
        long size = (Long) Files.getAttribute(path, "size");
        assertEquals(SIZE, size);
        channel = Files.newByteChannel(path, StandardOpenOption.WRITE);
        final long newSize = SIZE / 2;
        channel.truncate(newSize);
        channel.close();
        size = (Long) Files.getAttribute(path, "size");
        assertEquals(newSize, size);
    }
}
