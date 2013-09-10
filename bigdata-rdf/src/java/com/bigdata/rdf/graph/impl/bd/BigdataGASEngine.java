package com.bigdata.rdf.graph.impl.bd;

import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.sail.Sail;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Tuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.util.VertexDistribution;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.IBigdataFederation;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * {@link IGASEngine} for dynamic activation of vertices. This implementation
 * maintains a frontier and lazily initializes the vertex state when the vertex
 * is visited for the first time. This is appropriate for algorithms, such as
 * BFS, that use a dynamic frontier.
 * 
 * <h2>Dynamic Graphs</h2>
 * 
 * There are at least two basic approaches to computing an analytic over a
 * dynamic graph.
 * <p>
 * The first way to compute an analytic over a dynamic graph is to specify the
 * timestamp of the view as {@link ITx#READ_COMMITTED}. The view of the graph
 * <em>in each round</em> will be automatically advanced to the most recently
 * committed view of that graph. Thus, if there are concurrent commits, each
 * time the {@link IGASProgram} is executed within a given round of evaluation,
 * it will see the most recently committed state of the data graph.
 * <p>
 * The second way to compute an analytic over a dynamic graph is to explicitly
 * change the view before each round. This can be achieved by tunneling the
 * {@link BigdataGraphAccessor} interface from
 * {@link IGASProgram#nextRound(IGASContext)}. If you take this approach, then
 * you could explicitly walk through an iterator over the commit record index
 * and update the timestamp of the view. This approach allows you to replay
 * historical committed states of the graph at a known one-to-one rate (one
 * graph state per round of the GAS computation).
 * 
 * TODO Algorithms that need to visit all vertices in each round (CC, BC, PR)
 * can be more optimially executed by a different implementation strategy. The
 * vertex state should be arranged in a dense map (maybe an array) and presized.
 * For example, this could be done on the first pass when we identify a vertex
 * index for each distinct V in visitation order.
 * 
 * TODO Vectored expansion with conditional materialization of attribute values
 * could be achieved using CONSTRUCT. This would force URI materialization as
 * well. If we drop down one level, then we can push in the frontier and avoid
 * the materialization. Or we can just write an operator that accepts a frontier
 * and returns the new frontier and which maintains an internal map containing
 * both the visited vertices, the vertex state, and the edge state.
 * 
 * TODO Some computations could be maintained and accelerated. A great example
 * is Shortest Path (as per RDF3X). Reachability queries for a hierarchy can
 * also be maintained and accelerated (again, RDF3X using a ferrari index).
 * 
 * TODO Option to materialize Literals (or to declare the set of literals of
 * interest) [Note: We can also require that people inline all URIs and Literals
 * if they need to have them materialized, but a materialization filter for
 * Gather and Scatter would be nice if it can be selective for just those
 * attributes or vertex identifiers that matter).
 * 
 * TODO DYNAMIC GRAPHS: Another possibility would be to replay a history log,
 * explicitly making changes to the graph. In order to provide high concurrency
 * for readers, this would require a shadowing of the graph (effectively,
 * shadowing the indices). That might be achieved by replaying the changes into
 * a version fork of the graph and then using a read-only view of the fork. This
 * is basically a retroactive variant of replaying the commit points from the
 * commit record index. I am not sure if it has much to recommend it.
 * <p>
 * The thing that is interesting about the history index, is that it captures
 * just the delta. Actually computing the delta between two commit points is
 * none-trivial without the history index. However, I am not sure how we can
 * leverage that delta in an interesting fashion for dynamic graphs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataGASEngine extends GASEngine {

    private static final Logger log = Logger.getLogger(GASEngine.class);

//    /**
//     * The {@link IIndexManager} is used to access the graph.
//     */
//    private final IIndexManager indexManager;

    /**
     * Convenience constructor
     * 
     * @param sail
     *            The sail (must be a {@link BigdataSail}).
     * @param nthreads
     *            The number of threads to use for the SCATTER and GATHER
     *            phases.
     */
    public BigdataGASEngine(final Sail sail, final int nthreads) {
     
        this(((BigdataSail) sail).getDatabase().getIndexManager(), nthreads);
        
    }
    
    /**
     * 
     * @param indexManager
     *            The index manager.
     * @param nthreads
     *            The number of threads to use for the SCATTER and GATHER
     *            phases.
     * 
     *            TODO Scale-out: The {@link IIndexmanager} MAY be an
     *            {@link IBigdataFederation}. The {@link BigdataGASEngine} would
     *            automatically use remote indices. However, for proper
     *            scale-out we want to partition the work and the VS/ES so that
     *            would imply a different {@link IGASEngine} design.
     */
    public BigdataGASEngine(final IIndexManager indexManager, final int nthreads) {

        super(nthreads);
        
        if (indexManager == null)
            throw new IllegalArgumentException();

//        this.indexManager = indexManager;

    }

    @Override
    public <VS, ES, ST> IGASState<VS, ES, ST> newGASState(
            final IGraphAccessor graphAccessor,
            final IGASProgram<VS, ES, ST> gasProgram) {

        final IStaticFrontier frontier = newStaticFrontier();

        final IGASSchedulerImpl gasScheduler = newScheduler();

        return new BigdataGASState<VS, ES, ST>(this,
                (BigdataGraphAccessor) graphAccessor, frontier, gasScheduler,
                gasProgram);

    }

    /**
     * Returns <code>true</code> since the IOs will be vectored if the
     * frontier is sorted.
     */
    @Override
    public boolean getSortFrontier() {
        return true;
    }

    static public class BigdataGraphAccessor implements IGraphAccessor {

        private final IIndexManager indexManager;
        private final String namespace;
        private final long timestamp;
        private volatile WeakReference<AbstractTripleStore> kbRef;
        
        /**
         * 
         * @param indexManager
         *            The index manager.
         * @param namespace
         *            The namespace of the graph.
         * @param timestamp
         *            The timestamp of the view. If you specify
         *            {@link ITx#READ_COMMITTED}, then
         *            {@link IIndexManager#getLastCommitTime()} will be used to
         *            locate the most recently committed view of the graph at
         *            the start of each iteration. This provides one mechanism
         *            for dynamic graphs.
         */
        public BigdataGraphAccessor(final IIndexManager indexManager,
                final String namespace, final long timestamp) {

            if(indexManager == null)
                throw new IllegalArgumentException();
            
            if (namespace == null)
                throw new IllegalArgumentException();

            this.indexManager = indexManager;
            this.namespace = namespace;
            this.timestamp = timestamp;

        }

        /**
         * Obtain a view of the default KB instance as of the last commit time on
         * the database.
         */
        public BigdataGraphAccessor(final IIndexManager indexManager) {

            this(indexManager, BigdataSail.Options.DEFAULT_NAMESPACE,
                    indexManager.getLastCommitTime());

        }

        @Override
        public void advanceView() {

            if (this.timestamp == ITx.READ_COMMITTED) {

                /*
                 * Clear the reference. A new view will be obtained
                 * automatically by getKB().
                 */
                
                kbRef = null;

            }
            
        }

        /**
         * Return a view of the specified graph (aka KB) as of the specified
         * timestamp.
         * 
         * @return The graph.
         * 
         * @throws RuntimeException
         *             if the graph could not be resolved.
         */
        public AbstractTripleStore getKB() {

            if (kbRef == null) {

                synchronized (this) {

                    if (kbRef == null) {

                        kbRef = new WeakReference<AbstractTripleStore>(
                                resolveKB());

                    }

                }
            }

            return kbRef.get();

        }

        private AbstractTripleStore resolveKB() {

            long timestamp = this.timestamp;

            if (timestamp == ITx.READ_COMMITTED) {

                /**
                 * Note: This code is request the view as of the the last commit
                 * time. If we use ITx.READ_COMMITTED here then it will cause
                 * the Journal to provide us with a ReadCommittedIndex and that
                 * has a synchronization hot spot!
                 */

                timestamp = indexManager.getLastCommitTime();

            }

            final AbstractTripleStore kb = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, timestamp);

            if (kb == null) {

                throw new RuntimeException("Not found: namespace=" + namespace
                        + ", timestamp=" + TimestampUtility.toString(timestamp));

            }

            return kb;

        }

        public String getNamespace() {
         
            return namespace;
            
        }

        public Long getTimestamp() {
            
            return timestamp;
            
        }

        private final SPOKeyOrder getKeyOrder(final AbstractTripleStore kb,
                final boolean inEdges) {
            final SPOKeyOrder keyOrder;
            if (inEdges) {
                // in-edges: OSP / OCSP : [u] is the Object.
                keyOrder = kb.isQuads() ? SPOKeyOrder.OCSP : SPOKeyOrder.OSP;
            } else {
                // out-edges: SPO / (SPOC|SOPC) : [u] is the Subject.
                keyOrder = kb.isQuads() ? SPOKeyOrder.SPOC : SPOKeyOrder.SPO;
            }
            return keyOrder;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IStriterator getEdges(final AbstractTripleStore kb,
                final boolean inEdges, final IGASContext<?, ?, ?> ctx,
                final IV u) {

            final SPOKeyOrder keyOrder;
            final IIndex ndx;
            final IKeyBuilder keyBuilder;
            final IV linkTypeIV = (IV) ctx.getGASProgram().getLinkType();
            /*
             * Optimize case where P is a constant and O is known (2 bound).
             * 
             * P is a constant.
             * 
             * [u] gets bound on O.
             * 
             * We use the POS(C) index. The S values give us the out-edges for
             * that [u] and the specified link type.
             * 
             * FIXME POS OPTIMIZATION: write unit test for this option to make
             * sure that the right filter is imposed. write performance test to
             * verify expected benefit. Watch out for the in-edges vs out-edges
             * since only one is optimized.
             */
            final boolean posOptimization = linkTypeIV != null
                    && !inEdges;

            if (posOptimization) {
            
                /*
                 * POS(C)
                 */
                keyOrder = kb.isQuads() ? SPOKeyOrder.POCS : SPOKeyOrder.POS;

                ndx = kb.getSPORelation().getIndex(keyOrder);

                keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

                keyBuilder.reset();

                // Bind P as a constant.
                IVUtility.encode(keyBuilder, linkTypeIV);

                // Bind O for this key-range scan.
                IVUtility.encode(keyBuilder, u);
                
            } else {
                
                /*
                 * SPO(C) or OSP(C)
                 */
                
                keyOrder = getKeyOrder(kb, inEdges);

                ndx = kb.getSPORelation().getIndex(keyOrder);

                keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

                keyBuilder.reset();

                IVUtility.encode(keyBuilder, u);

            }

            final byte[] fromKey = keyBuilder.getKey();

            final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

            final ITupleIterator<ISPO> titr = ndx.rangeIterator(fromKey, toKey,
                    0/* capacity */, IRangeQuery.DEFAULT, null/* filter */);

            final IStriterator sitr = new Striterator(titr);

            sitr.addFilter(new Resolver() {
                private static final long serialVersionUID = 1L;
                @Override
                protected Object resolve(final Object e) {
                    final ITuple<ISPO> t = (ITuple<ISPO>) e;
                    return t.getObject();
                }
            });

            if (linkTypeIV != null && !posOptimization) {
                /*
                 * A link type constraint was specified, but we were not able to
                 * use the POS(C) index optimization. In this case we have to
                 * add a filter to impose that link type constraint.
                 */
                sitr.addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public boolean isValid(final Object e) {
                        return ((ISPO) e).p().equals(linkTypeIV);
                    }
                });
            }
            
            /*
             * Optionally wrap the program specified filter. This filter will be
             * pushed down onto the index. If the index is remote, then this is
             * much more efficient. (If the index is local, then simply stacking
             * striterators is just as efficient.)
             */
            return ((IGASProgram) ctx.getGASProgram()).constrainFilter(ctx,
                    sitr);

        }

        @SuppressWarnings("unchecked")
        private Iterator<ISPO> _getEdges(final IGASContext<?, ?, ?> p,
                @SuppressWarnings("rawtypes") final IV u, final EdgesEnum edges) {

            final AbstractTripleStore kb = getKB();
            
            switch (edges) {
            case NoEdges:
                return EmptyIterator.DEFAULT;
            case InEdges:
                return getEdges(kb, true/* inEdges */, p, u);
            case OutEdges:
                return getEdges(kb, false/* inEdges */, p, u);
            case AllEdges: {
                final IStriterator a = getEdges(kb, true/* inEdges */, p, u);
                final IStriterator b = getEdges(kb, false/* outEdges */, p, u);
                a.append(b);
                return a;
            }
            default:
                throw new UnsupportedOperationException(edges.name());
            }

        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public Iterator<Statement> getEdges(final IGASContext<?, ?, ?> ctx,
                final Value u, final EdgesEnum edges) {

            return (Iterator) _getEdges(ctx, (IV) u, edges);

        }

        @Override
        public VertexDistribution getDistribution(final Random r) {

            final VertexDistribution sample = new VertexDistribution(r);

            @SuppressWarnings("unchecked")
            final ITupleIterator<ISPO> titr = getKB().getSPORelation()
                    .getPrimaryIndex().rangeIterator();

            while (titr.hasNext()) {

                final ISPO spo = titr.next().getObject();

                if (!(spo.o().isResource())) {

                    // Not an edge.
                    continue;
                }

                sample.addSample((Resource) spo.s());

                sample.addSample((Resource) spo.o());

            }

            return sample;

        }

        /**
         * Return a sample (without duplicates) of vertices from the graph.
         * <p>
         * Note: This sampling procedure has a bias in favor of the vertices
         * with the most edges and properties (vertices are choosen randomly in
         * proportion to the #of edges and properties for the vertex).
         * 
         * @param desiredSampleSize
         *            The desired sample size.
         * 
         * @return The distinct samples that were found.
         * 
         *         TODO This is a (MUCH) more efficient way to create a biased
         *         sample. However, having a representative sample is probably
         *         more important than being efficient. Keep this code or
         *         discard? Maybe we could refactor the code to produce a
         *         {@link VertexDistribution} that was a sample of the total
         *         population, from which we then took a sample using the
         *         {@link VertexDistribution} API? That could be useful for
         *         large graphs (better than scanning the SPO(C) index!)
         */
        @SuppressWarnings("rawtypes")
        private IV[] getRandomSample(final Random r,
                final AbstractTripleStore kb, final int desiredSampleSize) {

            // Maximum number of samples to attempt.
            final int limit = (int) Math.min(desiredSampleSize * 3L,
                    Integer.MAX_VALUE);

            final Set<IV> samples = new HashSet<IV>();

            int round = 0;
            while (samples.size() < desiredSampleSize && round++ < limit) {

                final IV iv = getRandomVertex(r, kb);

                samples.add(iv);

            }

            return samples.toArray(new IV[samples.size()]);

        }

        /**
         * Return a random vertex.
         * <p>
         * The vertex is sampled in proportion to the number of statements
         * having that vertex as a subject, hence the sample is proportional to
         * the #of property values and out-links for a given vertex (in-links
         * are not considered by this sampling procedure).
         */
        @SuppressWarnings("rawtypes")
        private IV getRandomVertex(final Random r, final AbstractTripleStore kb) {

            /*
             * TODO This assumes a local, non-sharded index. The specific
             * approach to identifying a starting vertex relies on the
             * ILinearList API. If the caller is specifying the starting vertex
             * then we do not need to do this.
             */
            final BTree ndx = (BTree) kb.getSPORelation().getPrimaryIndex();

            // Select a random starting vertex.
            IV startingVertex = null;
            {

                // Truncate at MAX_INT.
                final int size = (int) Math.min(ndx.rangeCount(),
                        Integer.MAX_VALUE);

                while (size > 0L && startingVertex == null) {

                    final int rindex = r.nextInt(size);

                    /*
                     * Use tuple that will return both the key and the value so
                     * we can decode the entire tuple.
                     */
                    final Tuple<ISPO> tuple = new Tuple<ISPO>(ndx,
                            IRangeQuery.KEYS | IRangeQuery.VALS);

                    if (ndx.valueAt(rindex, tuple) == null) {

                        /*
                         * This is a deleted tuple. Try again.
                         * 
                         * Note: This retry is NOT safe for production use. The
                         * index might not have any undeleted tuples. However,
                         * we should not be using an index without any undeleted
                         * tuples for a test harness, so this should be safe
                         * enough here. If you want to use this production, at a
                         * mimimum make sure that you limit the #of attempts for
                         * the outer loop.
                         */
                        continue;

                    }

                    // Decode the selected edge.
                    final ISPO edge = (ISPO) ndx.getIndexMetadata()
                            .getTupleSerializer().deserialize(tuple);

                    // Use the subject for that edge as the starting vertex.
                    startingVertex = edge.s();

                    if (log.isInfoEnabled())
                        log.info("Starting vertex: " + startingVertex);

                }

            }

            if (startingVertex == null)
                throw new RuntimeException("No starting vertex: nedges="
                        + ndx.rangeCount());

            return startingVertex;

        }

    }


} // BigdataGASEngine