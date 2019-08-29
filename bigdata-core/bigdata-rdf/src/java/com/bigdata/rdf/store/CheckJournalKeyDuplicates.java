/*
This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.rdf.store;

import static java.lang.Thread.currentThread;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;


/**
 * A utility class to check integrity of an {@link AbstractTripleStore}.
 *
 * @author <a href="mailto:igorkim78@gmail.com"> Igor Kim </a>
 */


public class CheckJournalKeyDuplicates {

    protected static final Logger log = Logger.getLogger(CheckJournalKeyDuplicates.class);

    /**
     * Utility method to check indices in a local journal for key duplications.
     *
     * @param args
     *            <code>[-namespace <i>namespace</i>] propertyFile</code>
     *            where
     *            <dl>
     *            <dt>-checkStatements</dt>
     *            <dd>Checks statement indices for duplication as well. If not specified,
     *            the check will run only against lexicon indices.</dd>
     *            </dd>
     *            <dt>-checkUsingSet</dt>
     *            <dd>Checks statement indices for duplication as well. If not specified,
     *            the check will run only against lexicon indices.</dd>
     *            </dd>
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the KB instance. If not specified,
     *            the check will run against all the namespaces in the journal.</dd>
     *            </dd>
     *            <dt>propertyFile</dt>
     *            <dd>The configuration file for the database instance.</dd>
     *            </dl>
     */
    public static void main(final String[] args) {

        Banner.banner();
        Arguments arguments = Arguments.parseArguments(args);
        try {

            Optional<Journal> optionalJnl = arguments.openJournal();
            if (optionalJnl.isPresent()) {
                checkJournal(optionalJnl.get(), arguments.namespace, arguments.checkUsingSet, arguments.checkStatements);
            } else {
                log.debug("Journal " + arguments.getJournalPath() + " does not exist");
            }

        } catch (Throwable e) {
            log.error("Error", e);
        }
    }


    private static void checkJournal(Journal journal, String namespace, boolean checkUsingSet, boolean checkStatements) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        AtomicLong duplicates = new AtomicLong();
        Journal jnl = journal;
        try {
            log.info("Processing the journal: " + jnl.getFile());
            for (String nm : getNamespacesToCheck(jnl, namespace)) {
                checkNamespace(jnl, nm, duplicates, checkUsingSet, checkStatements);
            }
        } finally {
            jnl.close();
            log.info("Completed in " + stopwatch + ". Found " + duplicates.get() + " duplicates");
        }
    }

    private static List<String> getNamespacesToCheck(Journal jnl, String namespace) {
        if (namespace == null) {
            return jnl.getGlobalRowStore().getNamespaces(jnl.getLastCommitTime());
        }
        return singletonList(namespace);
    }

    private static void checkNamespace(final Journal jnl, final String namespace, final AtomicLong duplicates, final boolean checkUsingSet, final boolean checkStatements) throws Exception {
        log.debug("Copying " + namespace);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        log.info("Running verify (TripleStoreUtility.modelsEqual)");
        final ICommitRecord commitRecord = jnl.getCommitRecord();
        final Iterator<String> nitr = jnl.indexNameScan(null/* prefix */,
                commitRecord.getTimestamp());
        final List<Callable<Void>> tasks = new ArrayList<>();

        while (nitr.hasNext()) {
            final String name = nitr.next();
            if (name.startsWith(namespace) && (checkStatements || !name.contains(".spo."))) {
                log.debug("Processing "+name);
                final AbstractBTree ndx = jnl.getIndex(name);//WithCommitRecord , commitRecord);
                if (ndx != null) {
                    tasks.add(new Callable<Void>() {
                        @Override
                        public Void call() {
                            ITupleIterator<?> bitr = ndx.rangeIterator();
                            long processed = 0;
                            int n = 5_000_000;
                            long nextChunk = n;
                            final long startTime = System.currentTimeMillis();
                            long startTimeChunk = startTime;
                            Set<ByteBuffer> prevKeys = new HashSet<>();
                            Set<ByteBuffer> prevValues = new HashSet<>();
                            Term2IdTupleSerializer serTerm2Id = null;
                            Id2TermTupleSerializer serId2Term = null;
                            if (checkUsingSet) {
                                if (name.endsWith("TERM2ID")) {
                                    serTerm2Id = new Term2IdTupleSerializer();
                                } else if (name.endsWith("ID2TERM")) {
                                    serId2Term = new Id2TermTupleSerializer(name, new BigdataValueFactoryImpl());
                                }
                            }
                            byte[] prevKey = new byte[0];
                            byte[] prevValue = null;
                            while (bitr.hasNext()) {
                                ITuple<?> t = bitr.next();
                                byte[] key = t.getKey();
                                Object deserializedKey = null;
                                Object deserializedValue = null;
                                try {
                                    if (name.endsWith("TERM2ID")) {
                                        ByteBuffer tempArr = ByteBuffer.wrap(new byte[key.length+3]);
                                        byte[] keyFixed = tempArr.array();
                                        System.arraycopy(key, 1, keyFixed,0, key.length-1);
                                        deserializedKey = new String(keyFixed, StandardCharsets.US_ASCII); //bigdataValueSerializer.deserialize(keyFixed);
                                        deserializedValue = serTerm2Id.deserialize(t);
                                    } else if (name.endsWith("ID2TERM")) {
                                        deserializedKey = serId2Term.deserializeKey(t);
                                        deserializedValue = serId2Term.deserialize(t);
                                    }
                                    log.warn("Idx " + name + " Check " 
                                        + " tuple " + deserializedKey + " to " + deserializedValue
                                        + " key " + Arrays.toString(key)
                                        + " value " + Arrays.toString(t.getValue()));
                                } catch(Exception e) {
                                    log.error("Error",e);
                                }
                                if (checkUsingSet && (name.endsWith("TERM2ID") || name.endsWith("ID2TERM"))) {
                                    // wrap byte[] as ByteBuffer to provide it with proper hashcode and compare
                                    ByteBuffer keyAsBuffer = ByteBuffer.wrap(key);
                                    if (prevKeys.contains(keyAsBuffer)) {
                                        log.warn("Idx " + name + " Duplicated key in set " 
                                            + " tuple " + deserializedKey + " to " + deserializedValue
                                            + " key " + Arrays.toString(key)
                                            + " value " + Arrays.toString(t.getValue()));
                                        duplicates.incrementAndGet();
                                    }
                                    prevKeys.add(keyAsBuffer);
                                    // wrap byte[] as ByteBuffer to provide it with proper hashcode and compare
                                    ByteBuffer valueAsBuffer = ByteBuffer.wrap(t.getValue());
                                    if (prevValues.contains(valueAsBuffer)) {
                                        log.warn("Idx " + name + " Duplicated value in set " 
                                                + " tuple " + deserializedKey + " to " + deserializedValue
                                                + " key " + Arrays.toString(key)
                                                + " value " + Arrays.toString(t.getValue()));
                                        duplicates.incrementAndGet();
                                    }
                                    prevValues.add(valueAsBuffer);
                                }
                                if (Arrays.equals(prevKey, key)) {
                                    if (prevValue != null) {
                                        // Log the first duplicated record
                                        log.warn("Idx " + name + " Duplicated "
                                                + " key " + Arrays.toString(prevKey) 
                                                + " value " + Arrays.toString(prevValue));
                                        prevValue = null;
                                    }
                                    // Log the subsequent duplicated records
                                    log.warn("Idx " + name + " Duplicated " 
                                            + " key " + Arrays.toString(key) 
                                            + " value " + Arrays.toString(t.getValue()));
                                    duplicates.incrementAndGet();
                                } else {
                                    prevKey = key;
                                    prevValue = t.getValue();
                                }
                                processed ++;
                                if (processed >= nextChunk) {
                                    long currentTime = System.currentTimeMillis();
                                    long tps = 1000L*n/(currentTime-startTimeChunk);
                                    long avgTps = 1000L*processed/(currentTime-startTime);
                                    log.debug(name+"\t"+processed/1_000_000 + "M\ttps\t"+tps/1_000+"K\tavg\t"+avgTps/1_000+"K");
                                    startTimeChunk = currentTime;
                                    nextChunk+=n;
                                }
                            }
                            long currentTime = System.currentTimeMillis();
                            long tps = 1000L*n/(currentTime-startTimeChunk);
                            long avgTps = 1000L*processed/(currentTime-startTime);
                            log.info(name+"\t"+processed/1_000_000 + "M\ttps\t"+tps/1_000+"K\tavg\t"+avgTps/1_000+"K");
                            return null;
                        }
                    });
                }
            }
        }
        try {
            // Run all the tasks, awaiting their completion
            pool.invokeAll(tasks);
        } catch (InterruptedException e) {
            log.error("Error", e);
            Thread.currentThread().interrupt();
        }
        try {
            pool.shutdown();
            if (!pool.awaitTermination(1, MINUTES)) { // TODO: tune this timeout to what is actually needed
                pool.shutdownNow();
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            currentThread().interrupt();
        }
    }

    public static class Arguments {
        protected static final Logger log = Logger.getLogger(Arguments.class);

        @Nullable public final String namespace;
        private final String propertyFileName;
        private final boolean checkUsingSet;
        private final boolean checkStatements;

        private Arguments(@Nullable String namespace, String propertyFileName, boolean checkUsingSet, boolean checkStatements) {
            this.namespace = namespace;
            this.propertyFileName = propertyFileName;
            this.checkUsingSet = checkUsingSet;
            this.checkStatements = checkStatements;
        }

        public static Arguments parseArguments(String[] args) {
            String namespace = null;
            boolean checkUsingSet = false;
            boolean checkStatements = false;
            int i = 0;

            while (i < args.length) {
                final String arg = args[i];
                if (arg.startsWith("-")) {
                    switch (arg) {
                        case "-namespace":
                            namespace = args[++i];
                            break;
                        case "-checkUsingSet":
                            checkUsingSet = true;
                            break;
                        case "-checkStatements":
                            checkStatements = true;
                            break;
                        default:
                            log.debug("Unknown argument: " + arg);
                            usage();
                            break;
                    }
                } else {
                    break;
                }
                i++;
            }

            if (i >= args.length) {
                // No property filename
                System.err.println("Not enough arguments.");
                usage();
            }

            String propertyFileName = args[i];

            return new Arguments(namespace, propertyFileName, checkUsingSet, checkStatements);
        }

        private static void usage() {
            System.err.println("Usage: [-namespace namespace] propertyFile");
            System.exit(1);
        }

        public Properties loadProperties() throws IOException {
            final Properties properties = new Properties();

            try (InputStream is = newInputStream(getJournalPath(), READ)) {
                properties.load(is);
            }

            return properties;
        }

        public Path getJournalPath() {
            return Paths.get(propertyFileName);
        }

        public Optional<Journal> openJournal() throws IOException {
            final Properties properties = loadProperties();
            Path journal = Paths.get(properties.getProperty(Options.FILE));

            if (!isRegularFile(journal)) return Optional.absent();

            return Optional.of(new Journal(properties));
        }
    }
}
