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
    private static final int            FILE_COUNT      = 512;
    protected static final int          MAX_PIECE_COUNT = 8;
    private static final long           SEED            = System.currentTimeMillis();
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
        removeTestDirectory();
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
    public final void testToplogyDistribution() throws InterruptedException {
        final FilterServerMap topology = new FilterServerMap();
        final DistributedTrackerFiles admin = archive
                .getDistributedTrackerFiles(new InetSocketAddress(38800));
        admin.distribute(topology);
        admin.distribute(topology);
        Thread.sleep(1000);
        assertTrue(admin.getTopologyArchivePath().getAbsolutePath(TESTDIR)
                .toFile().exists());
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
     * @throws FileInfoMismatchException
     * @throws IOException
     */
    @Test
    public final void testPutPiece() throws FileInfoMismatchException,
            IOException {
        final Stopwatch stopwatch = new Stopwatch();
        int fileCount = 0;
        int pieceCount = 0;
        long byteCount = 0;
        final FileId prevFileId = null;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            final FileId fileId = piece.getFileInfo().getFileId();
            if (!fileId.equals(prevFileId)) {
                fileCount++;
            }
            stopwatch.start();
            archive.putPiece(piece);
            stopwatch.stop();
            pieceCount++;
            byteCount += piece.getSize();
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
     * @throws FileInfoMismatchException
     */
    @Test
    public final void testGetPiece() throws IOException,
            FileInfoMismatchException {
        final Stopwatch stopwatch = new Stopwatch();
        int fileCount = 0;
        int pieceCount = 0;
        long byteCount = 0;
        final FileId prevFileId = null;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            final FileId fileId = piece.getFileInfo().getFileId();
            if (!fileId.equals(prevFileId)) {
                fileCount++;
            }
            stopwatch.start();
            final Piece savedPiece = archive.getPiece(piece.getInfo());
            stopwatch.stop();
            pieceCount++;
            byteCount += savedPiece.getSize();
            assertEquals(piece, savedPiece);
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
     * @throws FileInfoMismatchException
     */
    @Test
    public final void testExists() throws IOException,
            FileInfoMismatchException {
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
     * @throws FileInfoMismatchException
     */
    @Test
    public final void testRemove() throws IOException,
            FileInfoMismatchException {
        final Piece piece = firstPiece();
        assertTrue(archive.exists(piece.getInfo()));
        archive.remove(piece.getArchivePath());
        assertFalse(archive.exists(piece.getInfo()));
    }
}
