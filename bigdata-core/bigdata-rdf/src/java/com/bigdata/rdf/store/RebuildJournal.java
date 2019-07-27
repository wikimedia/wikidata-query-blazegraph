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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.spo.SPOKeyOrder;


/**
 * A utility class to rebuild journal from an {@link AbstractTripleStore}
 * with the same configuration for the source and target journals.
 *
 * Rebuild procedure might be used to sweep unused sparse space from the journal,
 * which results in journal file compacting and reduction of allocators used.
 * It runs faster than unloading and loading triples via interchange format,
 * as it does not actually do any conversion of the index data of the source journal,
 * instead it concurrently runs iterators over each index and insert tuples as-is
 * into the new journal. So it could not recode to new inlining configuration nor
 * use other vocabulary than the original journal uses.
 *
 * @author <a href="mailto:igorkim78@gmail.com"> Igor Kim </a>
 */


public class RebuildJournal {

    protected static final Logger log = Logger.getLogger(RebuildJournal.class);

    /**
     * Utility method to rebuid a local journal.
     *
     * @param args
     *            <code>[-namespace <i>namespace</i>] [-dump] [-verify] propertyFile</code>
     *            where
     *            <dl>
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the KB instance.</dd>
     *            <dt>-extendSize</dt>
     *            <dd>The size of the extend provisioned for the new journal (if not specified,
     *                set to estimated target size 2/3 of the original journal size).</dd>
     *            <dt>-dump</dt>
     *            <dd>Flag to output content of the original and reloaded journal to GZIP file,
     *                containing all the triples with corresponding IVs (Useful for journals comparison).
     *                Note, that journal dump takes significant time and disk space. This flag should not
     *                be used for production.
     *            </dd>
     *            <dt>-verify</dt>
     *            <dd>Flag to test whether the old and the new journals contain the same triples.
     *            </dd>
     *            <dt>propertyFile</dt>
     *            <dd>The configuration file for the database instance.</dd>
     *            </dl>
     */
    public static void main(final String[] args) throws IOException {

        Banner.banner();

        String namespace = null;
        boolean dump = false;
        boolean verify = false;
        boolean rebuild = false;
        long extentSize = 0;

        int i = 0;

        while (i < args.length) {

            final String arg = args[i];

            if (arg.startsWith("-")) {

                switch (arg) {
                    case "-namespace":

                        namespace = args[++i];

                        break;
                    case "-dump":

                        dump = true;

                        break;
                    case "-extentSize":

                        extentSize = Long.parseLong(args[++i]);

                        break;
                    case "-rebuild":

                        rebuild = true;
                        break;
                    case "-verify":

                        verify = true;
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

        final String propertyFileName = args[i];
        try {
            final Properties properties = processProperties(propertyFileName);

            File journal = new File(properties.getProperty(Options.FILE));

            long startTime = System.currentTimeMillis();

            if (journal.isFile()) {


                Journal jnl = new Journal(properties);

                try {

                    dumpJournalStats(jnl);

                    log.info("Processing the journal: " + jnl.getFile());

                    String newJournalFile = properties.getProperty(Options.FILE).replace(".jnl", "-rebuilt.jnl");
                    Properties newProperties = new Properties(properties);
                    newProperties.setProperty(Options.FILE, newJournalFile);
                    String estimatedFileSize = Long.toString(extentSize > 0 ? extentSize : jnl.getFile().length()*2/3);
                    newProperties.setProperty(Options.INITIAL_EXTENT, estimatedFileSize);
                    newProperties.setProperty(Options.MAXIMUM_EXTENT, estimatedFileSize);
                    Journal dstjnl = null;

                    if (rebuild) {
                        dstjnl = new Journal(newProperties);
                        log.info("Rebuilding into: " + dstjnl.getFile());
                        // Ensure all the stores are consistent
                        dstjnl.getGlobalRowStore();
                        dstjnl.commit();
                    }

                    jnl.getGlobalRowStore();
                    jnl.commit();

                    if (namespace == null) {

                        List<String> namespaces = jnl.getGlobalRowStore().getNamespaces(jnl.getLastCommitTime());

                        for (String nm : namespaces) {

                            copyNamespace(jnl, nm, dstjnl, dump, rebuild, verify, newProperties);
                        }

                    } else {

                        copyNamespace(jnl, namespace, dstjnl, dump, rebuild, verify, newProperties);

                    }
                } finally {
                    jnl.commit();
                    jnl.close();
                    log.info("Completed in "+(System.currentTimeMillis()-startTime));
                }

            } else {

                log.debug("Journal " + journal + " does not exist");

            }

        } catch (Throwable e) {
            log.error("Error", e);
        }
    }

    private static void dumpJournalStats(Journal jnl) throws IOException {
        boolean dumpHistory = false;
        boolean dumpPages = false;
        boolean dumpIndices = false;
        boolean showTuples = false;
        final File statsName = new File(jnl.getProperties().getProperty(Options.FILE) + ".stats");
        log.info("Writing journal stats into: " + statsName);
        try (PrintWriter writer = new PrintWriter(new FileWriter(statsName))) {
            new DumpJournal(jnl).dumpJournal(writer, null/* namespaces */, dumpHistory, dumpPages, dumpIndices, showTuples);
        }
    }

    private static void copyNamespace(final Journal jnl, final String namespace,
            final Journal dstjnl, boolean dump, boolean rebuild, boolean verify, Properties newProperties) throws IOException {
        log.debug("Copying "+namespace);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        if (rebuild) {
            AbstractTripleStore dst = (AbstractTripleStore) dstjnl
                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);

            if (dst == null) {
                dst = new LocalTripleStore(dstjnl, namespace, ITx.UNISOLATED, newProperties);
                dst.create();
            } else {
                log.debug("Namespace " + namespace + " in the journal " +
                        dstjnl.getProperties().getProperty(Options.FILE) +
                        " already exists. Skipping rebuild.");
                dumpJournalStats(dstjnl);
                return;
            }

            final ICommitRecord commitRecord = jnl.getCommitRecord();
            final Iterator<String> nitr = jnl.indexNameScan(null/* prefix */,
                    commitRecord.getTimestamp());
            final List<Callable<Void>> tasks = new ArrayList<>();
            while (nitr.hasNext()) {
                final String name = nitr.next();
                if (name.startsWith(namespace)) {
                    log.debug("Processing "+name);
                    final ICheckpointProtocol ndx = jnl.getIndexWithCommitRecord(name, commitRecord);
                    final AbstractBTree dstndx = dstjnl.getIndex(name);
                    dstndx.removeAll();
                    if (ndx instanceof AbstractBTree) {
                        tasks.add(new Callable<Void>() {
                            @Override
                            public Void call() {
                                ITupleIterator<?> bitr = ((AbstractBTree)ndx).rangeIterator();
                                long inserted = 0;
                                int n = 5_000_000;
                                long nextChunk = n;
                                final long startTime = System.currentTimeMillis();
                                long startTimeChunk = startTime;
                                while (bitr.hasNext()) {
                                    ITuple<?> t = bitr.next();
                                    dstndx.insert(t.getKey(), t.getValue());
                                    inserted ++;
                                    if (inserted >= nextChunk) {
                                        long currentTime = System.currentTimeMillis();
                                        long tps = 1000L*n/(currentTime-startTimeChunk);
                                        long avgTps = 1000L*inserted/(currentTime-startTime);
                                        log.debug(name+"\t"+inserted/1_000_000 + "M\ttps\t"+tps+"\tavg\t"+avgTps);
                                        startTimeChunk = currentTime;
                                        nextChunk+=n;
                                    }
                                }
                                return null;
                            }
                        });
                    }
                }
            }
            try {
                pool.invokeAll(tasks);
            } catch (InterruptedException e) {
                log.error("Error", e);
            }
            dst.commit();
            dstjnl.commit();
        }
        final String dstFile = newProperties.getProperty(Options.FILE);
        boolean dstJnlExists = new File(dstFile).exists();
        if (dstJnlExists) {
            dumpJournalStats(dstjnl);
        }
        final List<Callable<Void>> tasks = new ArrayList<>();
        if (dump) {
            tasks.add(
                new Callable<Void>() {
                    @Override
                    public Void call() {
                        AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                        dumpKb(kb, jnl.getProperties().getProperty(Options.FILE));
                        return null;
                    }
                });
            if (dstJnlExists) {
                tasks.add(
                    new Callable<Void>() {
                        @Override
                        public Void call() {
                            AbstractTripleStore dstkb = (AbstractTripleStore) dstjnl
                                    .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                            dumpKb(dstkb, dstFile);
                            return null;
                        }
                    });
            }
        }
        if (verify && dstJnlExists) {
            tasks.add(
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                        AbstractTripleStore dstkb = (AbstractTripleStore) dstjnl
                                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                        log.info("Running verify (TripleStoreUtility.modelsEqual)");
                        if (TripleStoreUtility.modelsEqual(kb, dstkb, /* bnodesCompareByIVs */ true)) {
                            log.info("Models are equal");
                        } else {
                            log.info("Models are not equal");
                        }
                        return null;
                    }

            });
        }
        try {
            List<Future<Void>> results = pool.invokeAll(tasks);
            for (Future<Void> result: results) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    log.error("Error", e);
                }
            }
        } catch (InterruptedException e) {
            log.error("Error", e);
        }
        pool.shutdownNow();
    }

    private static void dumpKb(AbstractTripleStore kb, String fileName) {
        String dumpName = fileName + ".data.gz";
        log.info("Dumping journal into "+dumpName);
        try (Writer w = new OutputStreamWriter(
                new GZIPOutputStream(new FileOutputStream(dumpName)))
                ) {
            final BigdataStatementIterator itr = kb
                    .asStatementIterator(kb.getAccessPath(SPOKeyOrder.SPO)
                            .iterator());
            while (itr.hasNext()) {
                BigdataStatement r = itr.next();
                w.write(r.toString() + " "
                        + r.getSubject().getIV() + " "
                        + r.getPredicate().getIV() + " "
                        + r.getObject().getIV() + "\n");
            }
        } catch (IOException e) {
            log.error("Error", e);
        }
    }

    private static Properties processProperties(final String propertyFileName) throws IOException {

        final File propertyFile = new File(propertyFileName);

        final Properties properties = new Properties();

        try (InputStream is = new FileInputStream(propertyFile)) {
            properties.load(is);
        }

        return properties;
    }

    private static void usage() {

        System.err.println("Usage: [-namespace namespace] [-dump] [-verify] [-extentSize dstJournalSize] propertyFile");

        System.exit(1);

    }

}
