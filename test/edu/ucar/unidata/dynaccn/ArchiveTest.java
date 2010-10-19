/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE in the top-level source directory for licensing
 * information.
 */
package edu.ucar.unidata.dynaccn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

    private static final String   ROOT_DIR   = "/tmp/"
                                                     + ArchiveTest.class
                                                             .getSimpleName();
    private static final int      FILE_COUNT = 2048;
    private static final long     SEED       = System.currentTimeMillis();

    private Random                random;
    private final List<PieceSpec> pieceSpecs = new LinkedList<PieceSpec>();
    private Archive               archive;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Assert.assertEquals(0, Misc.system("rm", "-rf", ROOT_DIR));
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Assert.assertEquals(0, Misc.system("rm", "-rf", ROOT_DIR));
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        archive = new Archive(ROOT_DIR);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        archive.close();
    }

    private Piece firstPiece() {
        pieceSpecs.clear();
        random = new Random(SEED);
        final int maxPieceSize = FileInfo.getDefaultPieceSize();
        for (int i = 0; i < FILE_COUNT; i++) {
            final Path path = Paths.get(Integer.toString(i));
            final FileId fileId = new FileId(path);
            final long fileSize = (i == 0)
                    ? 0
                    : random.nextInt(maxPieceSize + 1);
            final FileInfo fileInfo = new FileInfo(fileId, fileSize);
            final PieceSpec pieceSpec = new PieceSpec(fileInfo, 0);
            pieceSpecs.add(pieceSpec);
        }
        return nextPiece();
    }

    private Piece nextPiece() {
        final int size = pieceSpecs.size();
        if (0 >= size) {
            return null;
        }
        // final PieceSpec pieceSpec = pieceSpecs.remove(random.nextInt(size));
        final PieceSpec pieceSpec = pieceSpecs.remove(0);
        final byte[] bytes = new byte[pieceSpec.getSize()];
        random.nextBytes(bytes);
        return new Piece(pieceSpec, bytes);
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.Archive#putPiece(edu.ucar.unidata.dynaccn.Piece)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testPutPiece() throws IOException {
        final Stopwatch stopwatch = new Stopwatch();
        int pieceCount = 0;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            stopwatch.start();
            archive.putPiece(piece);
            stopwatch.stop();
            pieceCount++;
        }
        System.out.println("testPutPiece():");
        System.out.println("    Number of pieces = " + pieceCount);
        System.out.println("    Net elapsed time = "
                + stopwatch.getElapsedTime() + " s");
        System.out.println("    Piece throughput = " + pieceCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("    Byte throughput  = "
                + (pieceCount * FileInfo.getDefaultPieceSize())
                / stopwatch.getElapsedTime() + "/s");
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.Archive#getPiece(edu.ucar.unidata.dynaccn.PieceSpec)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testGetPiece() throws IOException {
        final Stopwatch stopwatch = new Stopwatch();
        int pieceCount = 0;
        for (Piece piece = firstPiece(); piece != null; piece = nextPiece()) {
            stopwatch.start();
            final Piece savedPiece = archive.getPiece(piece.getInfo());
            stopwatch.stop();
            pieceCount++;
            assertEquals(piece, savedPiece);
        }
        System.out.println("testGetPiece():");
        System.out.println("    Number of pieces = " + pieceCount);
        System.out.println("    Net Elapsed time = "
                + stopwatch.getElapsedTime() + " s");
        System.out.println("    Piece throughput = " + pieceCount
                / stopwatch.getElapsedTime() + "/s");
        System.out.println("    Byte throughput  = "
                + (pieceCount * FileInfo.getDefaultPieceSize())
                / stopwatch.getElapsedTime() + "/s");
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.Archive#exists(edu.ucar.unidata.dynaccn.PieceSpec)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testExists() throws IOException {
        assertTrue(archive.exists(firstPiece().getInfo()));
        assertFalse(archive.exists(new PieceSpec(new FileInfo(new FileId(Paths
                .get("dummy")), 1), 0)));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.Archive#walkArchive(edu.ucar.unidata.dynaccn.FilePieceSpecSetConsumer, edu.ucar.unidata.dynaccn.Predicate)}
     * .
     */
    @Test
    public final void testWalkArchive() {
        class Consumer implements FilePieceSpecSetConsumer {
            int fileCount;

            @Override
            public void consume(final FilePieceSpecSet spec) {
                fileCount++;
            }
        }
        ;
        final Consumer consumer = new Consumer();
        archive.walkArchive(consumer, Predicate.EVERYTHING);
        assertEquals(FILE_COUNT, consumer.fileCount);
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.dynaccn.Archive#remove(edu.ucar.unidata.dynaccn.FileId)}
     * .
     * 
     * @throws IOException
     */
    @Test
    public final void testRemove() throws IOException {
        final Piece piece = firstPiece();
        archive.remove(new FileId(piece.getPath()));
        assertFalse(archive.exists(piece.getInfo()));
    }
}
