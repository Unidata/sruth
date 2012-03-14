import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileSystemsTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testInstalledProviders() {
        for (final FileSystemProvider provider : FileSystemProvider
                .installedProviders()) {
            System.out.println(provider.toString());
        }
    }

    @Test(expected = java.nio.file.ProviderNotFoundException.class)
    public void testNewFileSystem() throws IOException {
        final Path path = Paths.get("/tmp/FileSystem");
        final SeekableByteChannel channel = Files.newByteChannel(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        channel.position(999999);
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] { 0 });
        channel.write(buf);
        channel.close();
        FileSystems.newFileSystem(path, getClass().getClassLoader());
    }
}
