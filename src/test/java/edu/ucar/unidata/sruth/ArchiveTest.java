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
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
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

    private static final Path           TESTDIR         = Paths.get(
                                                                System.getProperty("java.io.tmpdir"))
                                                                .resolve(
                                                                        ArchiveTest.class
                                                                                .getSimpleName());
    private static final int            FILE_COUNT      = 256;
    protected static final int          MAX_PIECE_COUNT = 8;
    private static final long           SEED            = Long.MAX_VALUE;                         // System.currentTimeMillis();
    private static final ArchiveTime    archiveTime     = new ArchiveTime();

    private Random                      random;
    private Archive                     archive;
    private final LinkedList<PieceSpec> pieceSpecs      = new LinkedList<PieceSpec>();

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
        // removeTestDirectory();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        archive = new Archive(TESTDIR, FILE_COUNT / 4);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        archive.close();
    }

    /**
     * Returns the same first piece every time.
     * 
     * @return
     */
    private Piece firstPiece() {
        random = new Random(SEED);
        for (int i = 0; i < FILE_COUNT; i++) {
            final Path path = Paths.get(Integer.toString(i));
            final ArchivePath archivePath = new ArchivePath(path);
            final FileId fileId = new FileId(archivePath, archiveTime);
            final long fileSize = (i == 0)
                    ? 0
                    : random.nextInt(FileInfo.getDefaultPieceSize()
                            * MAX_PIECE_COUNT + 1);
            final FileInfo fileInfo = new FileInfo(fileId, fileSize);
            for (int j = 0; j < fileInfo.getPieceCount(); j++) {
                final PieceSpec pieceSpec = new PieceSpec(fileInfo, j);
                pieceSpecs.add(pieceSpec);
            }
        }
        return nextPiece();
    }

    /**
     * Returns the same i-th piece every time.
     * 
     * @return
     */
    private Piece nextPiece() {
        final int n = pieceSpecs.size();
        if (n <= 0) {
            return null;
        }
        final int i = random.nextInt(n);
        final PieceSpec pieceSpec = pieceSpecs.remove(i);
        final byte[] bytes = new byte[pieceSpec.getSize()];
        random.nextBytes(bytes);
        return new Piece(pieceSpec, bytes);
    }

    /**
     * Tests deleting a non-existent file
     * 
     * @throws FileSystemException
     *             if too many files are open
     * @throws IOException
     *             if an I/O error occurs
     */
    private final void testDeleteMissing() throws FileSystemException,
            IOException {
        final ArchivePath archivePath = new ArchivePath("doesn't exist");
        archive.remove(archivePath);
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
                "/tmp/PubSubTest/subscribers/1/SRUTH/gilda.unidata.ucar.edu:38800/Topology");
        final InputStream inputStream = new FileInputStream(file);
        final ObjectInputStream ois = new ObjectInputStream(inputStream);
        final Topology topology = (Topology) ois.readObject();
        assertNotNull(topology);
        ois.close();
    }

    private final void testToplogyDistribution() throws InterruptedException {
        final Topology topology = new Topology();
        final DistributedTrackerFiles admin = archive
                .getDistributedTrackerFiles(new InetSocketAddress(
                        Tracker.IANA_PORT));
        admin.distribute(topology);
        admin.distribute(topology);
        Thread.sleep(1000);
        assertTrue(admin.getTopologyArchivePath().getAbsolutePath(TESTDIR)
                .toFile().exists());
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.Archive#putPiece(edu.ucar.unidata.sruth.Piece)}
     * .
     * 
     * @throws FileInfoMismatchException
     * @throws IOException
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    private final void testPutPiece() throws FileInfoMismatchException,
            IOException, InterruptedException {
        final Stopwatch stopwatch = new Stopwatch();
        int pieceCount = 0;
        long byteCount = 0;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            stopwatch.start();
            archive.putPiece(piece);
            stopwatch.stop();
            pieceCount++;
            byteCount += piece.getSize();
        }
        System.out.println("testPutPiece():");
        System.out.println("    Number of");
        System.out.println("      Files  = " + FILE_COUNT);
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
     * @throws FileInfoMismatchException
     */
    private final void testGetPiece() throws IOException,
            FileInfoMismatchException {
        final Stopwatch stopwatch = new Stopwatch();
        int pieceCount = 0;
        long byteCount = 0;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            stopwatch.start();
            final Piece savedPiece = archive.getPiece(piece.getInfo());
            assertTrue(savedPiece != null);
            stopwatch.stop();
            pieceCount++;
            byteCount += savedPiece.getSize();
            assertEquals(piece, savedPiece);
        }
        System.out.println("testGetPiece():");
        System.out.println("    Number of");
        System.out.println("      Files  = " + FILE_COUNT);
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
     * @throws FileInfoMismatchException
     */
    private final void testExists() throws IOException,
            FileInfoMismatchException {
        assertTrue(archive.exists(firstPiece().getInfo()));
        final ArchivePath archivePath = new ArchivePath(
                Paths.get("doesn't exist"));
        final ArchiveTime archiveTime = new ArchiveTime();
        final FileId fileId = new FileId(archivePath, archiveTime);
        assertFalse(archive.exists(new PieceSpec(new FileInfo(fileId, 1), 0)));
    }

    /**
     * Test method for
     * {@link Archive#walkArchive(FilePieceSpecSetConsumer, Filter)} .
     * 
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if the current thread is interrupted.
     */
    private final void testWalkArchive() throws IOException,
            InterruptedException {
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
     * @throws FileInfoMismatchException
     * @throws InterruptedException
     *             if the current thread is interrupted
     */
    private final void testRemove() throws IOException,
            FileInfoMismatchException, InterruptedException {
        final Piece piece = firstPiece();
        archive.putPiece(piece);
        assertTrue(archive.exists(piece.getInfo()));
        final ArchivePath archivePath = piece.getArchivePath();
        archive.remove(archivePath);
        assertFalse(archive.exists(piece.getInfo()));
    }

    /**
     * Tests performance. Ensures that the archive is populated first and then
     * read.
     * 
     * @throws FileInfoMismatchException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public final void testArchive() throws FileInfoMismatchException,
            IOException, InterruptedException {
        testDeleteMissing();
        testToplogyDistribution();
        testPutPiece();
        testGetPiece();
        testExists();
        testWalkArchive();
        testRemove();
    }
}
