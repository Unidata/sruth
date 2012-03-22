import java.util.Iterator;
import java.util.NoSuchElementException;

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
class IteratorExperiment {
    static abstract class AbstractIterator<T> implements Iterator<T> {
        private final T next;

        AbstractIterator() {
            next = getNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        protected abstract T getNext();
    }

    static class StringList implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return new AbstractIterator<String>() {
                private final String string[] = { "initialized", null };
                private final int    i        = 0;

                @Override
                protected String getNext() {
                    return string[i];
                }
            };
        }
    }

    public static void main(final String[] args) {
        for (final Iterator<String> iter = new StringList().iterator(); iter
                .hasNext();) {
            System.out.println(iter.next());
        }
        for (final String string : new StringList()) {
            System.out.println(string);
        }
    }
}
