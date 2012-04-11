import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

/**
 * Copyright 2010 University Corporation for Atmospheric Research.  All rights
 * reserved.  See file LICENSE.txt in the top-level directory for licensing
 * information.
 */

/**
 * Test of the For-Each loop
 * 
 * @author Steven R. Emmerson
 */
class CryptoHashExperiment {
    private static long SEED = System.currentTimeMillis();

    public static void main(final String[] args)
            throws NoSuchAlgorithmException {
        final byte[] bytes = new byte[100000000];
        final Random random = new Random(SEED);
        random.nextBytes(bytes);
        final Stopwatch stopwatch = new Stopwatch();

        for (final String algorithm : new String[] { "MD2", "MD5", "SHA-1",
                "SHA-256", "SHA-384", "SHA-512" }) {
            final MessageDigest md = MessageDigest.getInstance(algorithm);
            stopwatch.reset();
            stopwatch.start();
            md.update(bytes);
            final byte[] digest = md.digest();
            stopwatch.stop();
            final double elapsedTime = stopwatch.getAccumulatedTime();
            System.out.println(algorithm + ":");
            System.out.println("  digest=" + Arrays.toString(digest));
            System.out.println("    time=" + elapsedTime + " s");
            System.out.println("    rate=" + bytes.length / elapsedTime
                    + " bytes/s");
            System.out.println("        =" + bytes.length * 8 / elapsedTime
                    + " bits/s");
        }
    }
}
