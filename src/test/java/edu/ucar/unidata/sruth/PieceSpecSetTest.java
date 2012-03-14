/**
 * Copyright 2012 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */
package edu.ucar.unidata.sruth;

import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Steven R. Emmerson
 */
public class PieceSpecSetTest {
    private PieceSpec           pieceSpec1;
    private PieceSpec           pieceSpec2;
    private FilePieceSpecs      filePieceSpecs1;
    private FilePieceSpecs      filePieceSpecs2;
    private MultiFilePieceSpecs multiFilePieceSpecs1;
    private MultiFilePieceSpecs multiFilePieceSpecs2;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        Path path;
        ArchivePath archivePath;
        FileId fileId;
        FileInfo fileInfo;

        path = Paths.get("pieceSpec1");
        archivePath = new ArchivePath(path);
        fileId = new FileId(archivePath);
        fileInfo = new FileInfo(fileId, 1);
        pieceSpec1 = new PieceSpec(fileInfo, 0);

        path = Paths.get("pieceSpec2");
        archivePath = new ArchivePath(path);
        fileId = new FileId(archivePath);
        fileInfo = new FileInfo(fileId, 2);
        pieceSpec2 = new PieceSpec(fileInfo, 0);

        path = Paths.get("filePieceSpec1");
        archivePath = new ArchivePath(path);
        fileId = new FileId(archivePath);
        fileInfo = new FileInfo(fileId, 1);
        filePieceSpecs1 = new FilePieceSpecs(fileInfo, true);

        path = Paths.get("filePieceSpec2");
        archivePath = new ArchivePath(path);
        fileId = new FileId(archivePath);
        fileInfo = new FileInfo(fileId, 2);
        filePieceSpecs2 = new FilePieceSpecs(fileInfo, true);

        multiFilePieceSpecs1 = (MultiFilePieceSpecs) filePieceSpecs1
                .merge(pieceSpec1);
        multiFilePieceSpecs2 = (MultiFilePieceSpecs) filePieceSpecs2
                .merge(pieceSpec2);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    private void verify(final PieceSpecSet o1, final PieceSpecSet o2,
            final PieceSpecSet result) {
        final HashSet<PieceSpec> set = new HashSet<PieceSpec>();
        for (final PieceSpec spec : o1) {
            set.add(spec);
        }
        for (final PieceSpec spec : o2) {
            set.add(spec);
        }
        for (final PieceSpec spec : result) {
            assertTrue(set.contains(spec));
        }
        set.clear();
        for (final PieceSpec spec : result) {
            set.add(spec);
        }
        for (final PieceSpec spec : o1) {
            assertTrue(set.contains(spec));
        }
        for (final PieceSpec spec : o2) {
            assertTrue(set.contains(spec));
        }
    }

    private void test(final PieceSpecSet o1, final PieceSpecSet o2) {
        final PieceSpecSet result = o1.merge(o2);
        verify(o1, o2, result);
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.PieceSpecSet#merge(edu.ucar.unidata.sruth.PieceSpecSet)}
     * .
     */
    @Test
    public final void testMergePieceSpecSet() {
        test(pieceSpec1, pieceSpec1);
        test(pieceSpec1, pieceSpec2);
        test(pieceSpec1, filePieceSpecs2);
        test(pieceSpec1, multiFilePieceSpecs2);

        test(filePieceSpecs1, pieceSpec2);
        test(filePieceSpecs1, filePieceSpecs1);
        test(filePieceSpecs1, filePieceSpecs2);
        test(filePieceSpecs1, multiFilePieceSpecs2);

        test(multiFilePieceSpecs1, pieceSpec2);
        test(multiFilePieceSpecs1, filePieceSpecs2);
        test(multiFilePieceSpecs1, multiFilePieceSpecs1);
        test(multiFilePieceSpecs1, multiFilePieceSpecs2);
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.PieceSpecSet#merge(edu.ucar.unidata.sruth.MultiFilePieceSpecs)}
     * .
     */
    @Test
    public final void testMergeMultiFilePieceSpecs() {
        verify(pieceSpec1, multiFilePieceSpecs1,
                pieceSpec1.merge(multiFilePieceSpecs1));
        verify(filePieceSpecs1, multiFilePieceSpecs1,
                filePieceSpecs1.merge(multiFilePieceSpecs1));
        verify(multiFilePieceSpecs1, multiFilePieceSpecs1,
                multiFilePieceSpecs1.merge(multiFilePieceSpecs1));
        verify(multiFilePieceSpecs1, multiFilePieceSpecs2,
                multiFilePieceSpecs1.merge(multiFilePieceSpecs2));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.PieceSpecSet#merge(edu.ucar.unidata.sruth.FilePieceSpecs)}
     * .
     */
    @Test
    public final void testMergeFilePieceSpecs() {
        verify(pieceSpec1, filePieceSpecs1, pieceSpec1.merge(filePieceSpecs1));
        verify(filePieceSpecs1, filePieceSpecs1,
                filePieceSpecs1.merge(filePieceSpecs1));
        verify(filePieceSpecs1, filePieceSpecs2,
                filePieceSpecs1.merge(filePieceSpecs2));
        verify(multiFilePieceSpecs1, filePieceSpecs1,
                multiFilePieceSpecs1.merge(filePieceSpecs1));
    }

    /**
     * Test method for
     * {@link edu.ucar.unidata.sruth.PieceSpecSet#merge(edu.ucar.unidata.sruth.PieceSpec)}
     * .
     */
    @Test
    public final void testMergePieceSpec() {
        verify(pieceSpec1, pieceSpec1, pieceSpec1.merge(pieceSpec1));
        verify(pieceSpec1, pieceSpec2, pieceSpec1.merge(pieceSpec2));
        verify(filePieceSpecs1, pieceSpec1, filePieceSpecs1.merge(pieceSpec1));
        verify(multiFilePieceSpecs1, pieceSpec1,
                multiFilePieceSpecs1.merge(pieceSpec1));
    }
}
