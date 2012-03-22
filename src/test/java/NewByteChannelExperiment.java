import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Tests the method {@link Path#newByteChannel()} for hanging when out of file
 * descriptors.
 * 
 * @author Steven R. Emmerson
 */
class NewByteChannelExperiment {
    public static void main(final String[] args) throws IOException {
        final List<SeekableByteChannel> channels = new LinkedList<SeekableByteChannel>();
        final Path rootDir = Paths.get("/tmp/NewByteChannelExperiment");
        Files.createDirectories(rootDir);

        for (int i = 0;; i++) {
            final Path path = rootDir.resolve(Integer.toString(i));
            channels.add(Files.newByteChannel(path, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE));
        }
    }
}
