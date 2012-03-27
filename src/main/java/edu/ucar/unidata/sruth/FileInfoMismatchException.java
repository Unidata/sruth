package edu.ucar.unidata.sruth;

/**
 * Indicates a mismatch between two file informations.
 * 
 * Instances are immutable.
 * 
 * @author Steven R. Emmerson
 */
final class FileInfoMismatchException extends Exception {
    /**
     * Serial version identifier
     */
    private static final long serialVersionUID = 1L;
    /**
     * The expected file information
     * 
     * @serial
     */
    private final FileInfo    expected;
    /**
     * The actual file information
     * 
     * @serial
     */
    private final FileInfo    actual;

    /**
     * Constructs from the expected and actual file informations.
     * 
     * @param expected
     *            The expected file information
     * @param actual
     *            The actual file information
     */
    FileInfoMismatchException(final FileInfo expected, final FileInfo actual) {
        this.expected = expected;
        this.actual = actual;
    }

    @Override
    public String toString() {
        return getClass().getCanonicalName() + "[expected=" + expected
                + ", actual=" + actual + "]";
    }
}