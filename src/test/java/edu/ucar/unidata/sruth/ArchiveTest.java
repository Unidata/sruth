/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.ucar.unidata.sruth.Archive.DistributedTrackerFiles;

/**
 * Tests the Archive class.
 * 
 * @author Steven R. Emmerson
 */
public class ArchiveTest {
    private class Stopwatch {
        private long startTime;
        private long elapsedTime;

        void start() {
            startTime = System.nanoTime();
        }

        void stop() {
            elapsedTime += System.nanoTime() - startTime;
        }

        double getElapsedTime() {
            return elapsedTime / 1e9;
        }
    }

    private static final Path        TESTDIR         = Paths.get(
                                                             System.getProperty("java.io.tmpdir"))
                                                             .resolve(
                                                                     ArchiveTest.class
                                                                             .getSimpleName());
    private static final int         FILE_COUNT      = 1024;
    protected static final int       MAX_PIECE_COUNT = 8;
    private static final long        SEED            = System.currentTimeMillis();
    private static final ArchiveTime archiveTime     = new ArchiveTime();

    private Random                   random;
    private Archive                  archive;
    private int                      pieceIndex;

    private static void removeTestDirectory() throws IOException,
            InterruptedException {
        Assert.assertEquals(0, Misc.system("rm", "-rf", TESTDIR.toString()));
    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        removeTestDirectory();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        removeTestDirectory();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        archive = new Archive(TESTDIR);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        archive.close();
    }

    /**
     * Returns a file iterator.
     */
    private Iterator<FileInfo> getFileIterator() {
        random = new Random(SEED);
        return new Iterator<FileInfo>() {
            private int index = 0;

            public boolean hasNext() {
                return index < FILE_COUNT;
            }

            public FileInfo next() {
                if (pieceIndex >= FILE_COUNT) {
                    throw new NoSuchElementException();
                }
                final Path path = Paths.get(Integer.toString(index));
                final ArchivePath archivePath = new ArchivePath(path);
                final FileId fileId = new FileId(archivePath, archiveTime);
                final long fileSize = (index == 0)
                        ? 0
                        : random.nextInt(FileInfo.getDefaultPieceSize()
                                * MAX_PIECE_COUNT + 1);
                index++;
                return new FileInfo(fileId, fileSize);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns a piece iterator.
     */
    private Iterator<Piece> getPieceIterator(final FileInfo fileInfo) {
        return new Iterator<Piece>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < fileInfo.getPieceCount();
            }

            @Override
            public Piece next() {
                if (index >= fileInfo.getPieceCount()) {
                    throw new NoSuchElementException();
                }
                final PieceSpec pieceSpec = new PieceSpec(fileInfo, index);
                final byte[] bytes = new byte[pieceSpec.getSize()];
                random.nextBytes(bytes);
                index++;
                return new Piece(pieceSpec, bytes);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the same first piece every time.
     * 
     * @return
     */
    private Piece firstPiece() {
        random = new Random(SEED);
        pieceIndex = 0;
        return nextPiece();
    }

    /**
     * Returns the same i-th piece every time.
     * 
     * @return
     */
    private Piece nextPiece() {
        if (pieceIndex >= FILE_COUNT) {
            return null;
        }
        final Path path = Paths.get(Integer.toString(pieceIndex));
        final ArchivePath archivePath = new ArchivePath(path);
        final FileId fileId = new FileId(archivePath, archiveTime);
        final long fileSize = (pieceIndex == 0)
                ? 0
                : random.nextInt(FileInfo.getDefaultPieceSize() + 1);
        final FileInfo fileInfo = new FileInfo(fileId, fileSize);
        final PieceSpec pieceSpec = new PieceSpec(fileInfo, 0);
        final byte[] bytes = new byte[pieceSpec.getSize()];
        random.nextBytes(bytes);
        pieceIndex++;
        return new Piece(pieceSpec, bytes);
    }

    /**
     * Tests getting new topology
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    // @Test
    public final void testTopology() throws IOException, ClassNotFoundException {
        final File file = new File(
                "/tmp/PubSubTest/subscribers/1/SRUTH/gilda.unidata.ucar.edu:38800/FilterServerMap");
        final InputStream inputStream = new FileInputStream(file);
        final ObjectInputStream ois = new ObjectInputStream(inputStream);
        final FilterServerMap topology = (FilterServerMap) ois.readObject();
        assertNotNull(topology);
        ois.close();
    }

    @Test
    public final void testToplogyDistribution() {
        final FilterServerMap topology = new FilterServerMap();
        final DistributedTrackerFiles admin = archive
                .getDistributedTrackerFiles(new InetSocketAddress(38800));
        admin.distribute(topology);
        admin.distribute(topology);
    }

    /**
     * Tests hiding a file in the archive.
     * 
     * @throws FileAlreadyExistsException
     * @throws IOException
     */
    @Test
    public final void testHide() throws FileAlreadyExistsException, IOException {
        final ArchivePath archivePath = new ArchivePath("hiddenFile");
        archive.hide(archivePath, archivePath);
        final Path path = archive.getHiddenAbsolutePath(archivePath.getPath());
        assertTrue(Files.exists(path));
        archive.removeHidden(archivePath);
        assertFalse(Files.exists(path));
    }

    /**
     * Tests purging the hidden directory.
     * 
     * @throws FileAlreadyExistsException
     * @throws IOException
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    @Test
    public final void testPurgeHiddenDir() throws FileAlreadyExistsException,
            IOException, InterruptedException {
        final ArchivePath archivePath = new ArchivePath("hiddenFile");
        archive.hide(archivePath, archivePath);
        archive.close();
        archive = new Archive(TESTDIR);
        Path path = archive.getHiddenAbsolutePath(archivePath.getPath());
        assertFalse(Files.exists(path));
        path = path.getParent();
        assertTrue(Files.exists(path));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Archive#putPiece(edu.ucar.unidata.sruth.Piece)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testPutPiece() throws IOException {
        final Stopwatch stopwatch = new Stopwatch();
        int fileCount = 0;
        int pieceCount = 0;
        long byteCount = 0;
        for (final Iterator<FileInfo> fileIter = getFileIterator(); fileIter
                .hasNext();) {
            fileCount++;
            for (final Iterator<Piece> pieceIter = getPieceIterator(fileIter
                    .next()); pieceIter.hasNext();) {
                final Piece piece = pieceIter.next();
                stopwatch.start();
                archive.putPiece(piece);
                stopwatch.stop();
                pieceCount++;
                byteCount += piece.getSize();
            }
        }
        System.out.println("testPutPiece():");
        System.out.println("    Number of");
        System.out.println("      Files  = " + fileCount);
        System.out.println("      Pieces = " + pieceCount);
        System.out.println("      Bytes  = " + byteCount);
        System.out.println("    Net elapsed time = "
                + stopwatch.getElapsedTime() + " s");
        System.out.println("    Throughput =");
        System.out.println("      Pieces: " + pieceCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("      Bytes:  " + byteCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("      Bits:   " + byteCount * 8
                / stopwatch.getElapsedTime() + "/s");
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Archive#getPiece(edu.ucar.unidata.sruth.PieceSpec)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testGetPiece() throws IOException {
        final Stopwatch stopwatch = new Stopwatch();
        int fileCount = 0;
        int pieceCount = 0;
        long byteCount = 0;
        for (final Iterator<FileInfo> fileIter = getFileIterator(); fileIter
                .hasNext();) {
            fileCount++;
            for (final Iterator<Piece> pieceIter = getPieceIterator(fileIter
                    .next()); pieceIter.hasNext();) {
                final Piece piece = pieceIter.next();
                stopwatch.start();
                final Piece savedPiece = archive.getPiece(piece.getInfo());
                stopwatch.stop();
                pieceCount++;
                byteCount += savedPiece.getSize();
                assertEquals(piece, savedPiece);
            }
        }
        System.out.println("testGetPiece():");
        System.out.println("    Number of");
        System.out.println("      Files  = " + fileCount);
        System.out.println("      Pieces = " + pieceCount);
        System.out.println("      Bytes  = " + byteCount);
        System.out.println("    Net Elapsed time = "
                + stopwatch.getElapsedTime() + " s");
        System.out.println("    Throughput =");
        System.out.println("      Pieces: " + pieceCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("      Bytes:  " + byteCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("      Bits:   " + byteCount * 8
                / stopwatch.getElapsedTime() + "/s");
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Archive#exists(edu.ucar.unidata.sruth.PieceSpec)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testExists() throws IOException {
        assertTrue(archive.exists(firstPiece().getInfo()));
        final ArchivePath archivePath = new ArchivePath(
                Paths.get("doesn't exist"));
        final FileTime fileTime = FileTime.fromMillis(System
                .currentTimeMillis());
        final ArchiveTime archiveTime = new ArchiveTime(fileTime);
        final FileId fileId = new FileId(archivePath, archiveTime);
        assertFalse(archive.exists(new PieceSpec(new FileInfo(fileId, 1), 0)));
    }

    /**
     * Test method for
     * {@link Archive#walkArchive(FilePieceSpecSetConsumer, Filter)} .
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Test
    public final void testWalkArchive() throws IOException {
        class Consumer implements FilePieceSpecSetConsumer {
            int fileCount;

            @Override
            public void consume(final FilePieceSpecSet spec) {
                fileCount++;
            }
        }
        ;
        final Consumer consumer = new Consumer();
        archive.walkArchive(consumer, Filter.EVERYTHING);
        assertEquals(FILE_COUNT + 1, consumer.fileCount); // + topology-file
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Archive#remove(edu.ucar.unidata.sruth.ArchivePath)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testRemove() throws IOException {
        final Piece piece = firstPiece();
        assertTrue(archive.exists(piece.getInfo()));
        archive.remove(piece.getArchivePath());
        assertFalse(archive.exists(piece.getInfo()));
    }
}
