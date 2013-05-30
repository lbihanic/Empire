/*
 * Copyright (c) 2009-2012 Clark & Parsia, LLC. <http://www.clarkparsia.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clarkparsia.empire.ds;

import com.clarkparsia.empire.ds.impl.TripleSourceAdapter;
import com.clarkparsia.empire.Dialect;
import com.clarkparsia.empire.SupportsRdfId;
import com.clarkparsia.empire.util.EmpireUtil;
import com.clarkparsia.empire.impl.serql.SerqlDialect;
import com.clarkparsia.empire.impl.sparql.ARQSPARQLDialect;

import com.clarkparsia.openrdf.Graphs;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.base.Function;

import org.openrdf.model.Resource;
import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.BNode;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;

import org.openrdf.query.BindingSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;

/**
 * <p>Collection of utility methods for working with Empire DataSources</p>
 *
 * @author	Michael Grove
 *
 * @since	0.7
 * @version	0.7.1
 *
 * @see DataSource
 * @see TripleSource
 */
public final class DataSourceUtil {
	/**
	 * The logger
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceUtil.class);

	/**
	 * No instances
	 */
	private DataSourceUtil() {
	}

	/**
	 * <p>Returns the given {@link DataSource} as a {@link TripleSource}.  If the DataSource does not natively support
	 * the interface, a wrapper is provided that delegates the triple level calls to SPARQL queries.</p>
	 * @param theSource the source
	 * @return the DataSource as a TripleSource.
	 * @see TripleSourceAdapter
	 * @throws DataSourceException if the TripleSource cannot be created.
	 */
	public static TripleSource asTripleSource(DataSource theSource) throws DataSourceException {
		if (theSource == null) {
			throw new DataSourceException("Cannot create triple source from null data source");
		}

		if (theSource instanceof TripleSource) {
			return (TripleSource) theSource;
		}
		else {
			return new TripleSourceAdapter(theSource);
		}
	}

	/**
	 * Do a poor-man's describe on the given resource, querying its context if that is supported, or otherwise
	 * querying the graph in general.
	 * @param theSource the {@link com.clarkparsia.empire.ds.DataSource} to query
	 * @param theObj the object to do the "describe" operation on
	 * @return all the statements about the given object
	 * @throws QueryException if there is an error while querying for the graph
	 */
	public static Graph describe(DataSource theSource, Object theObj) throws QueryException {
		java.net.URI aNG = null;

		if (EmpireUtil.asSupportsRdfId(theObj).getRdfId() == null) {
			return Graphs.newGraph();
		}

		if (theSource instanceof SupportsNamedGraphs && EmpireUtil.hasNamedGraphSpecified(theObj)) {
			aNG = EmpireUtil.getNamedGraph(theObj);
		}

		Dialect aDialect = theSource.getQueryFactory().getDialect();

		Resource aResource = EmpireUtil.asResource(EmpireUtil.asSupportsRdfId(theObj));

		// bnode instabilty in queries will just yield either a parse error or incorrect query results because the bnode
		// will get treated as a variable, and it will just grab the entire database, which is not what we want
		if (aResource instanceof BNode && !(aDialect instanceof ARQSPARQLDialect)) {
			return Graphs.newGraph();
		}

		Graph aGraph;
		// if source supports describe queries, use that.
		if (theSource instanceof TripleSource) {
			try {
				URI aNgUri = (aNG != null)? new URIImpl(aNG.toString()): null;
				aGraph = Graphs.newGraph(((TripleSource)theSource).getStatements(aResource, null, null, aNgUri));
			}
			catch (Exception e) {
				throw new QueryException(e);
			}
		}
		else {
			final String aUri = aDialect.asQueryString(aResource);
			String aQuery;
			if (aDialect instanceof SerqlDialect) {
				aQuery = "construct {s} p {o}\n" +
					 (aNG == null ? "from\n" : "from context <" + aNG + ">\n") +
					 "{s} p {o} where s = " + aUri + "";
			}
			else {
				// fall back on sparql
				aQuery = "construct {" + aUri + " ?p ?o}\n" +
					 (aNG == null ? "" : "from <" + aNG + ">\n") +
					 "where {" + aUri + " ?p ?o. }";
			}
			aGraph = theSource.graphQuery(aQuery);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Describe {}: {} triples", aResource, Integer.valueOf(aGraph.size()));
		}
		return aGraph;
	}

	/**
	 * Do a poor-man's ask on the given resource to see if any triples using the resource (as the subject) exist,
	 * querying its context if that is supported, or otherwise querying the graph in general.
	 * @param theSource the {@link com.clarkparsia.empire.ds.DataSource} to query
	 * @param theObj the object to do the "ask" operation on
	 * @return true if there are statements about the object, false otherwise
	 * @throws QueryException if there is an error while querying for the graph
	 */
	public static boolean exists(DataSource theSource, Object theObj) throws QueryException {
		SupportsRdfId aKey = EmpireUtil.asSupportsRdfId(theObj);
		if (aKey.getRdfId() == null) {
			return false;
		}

		String aNG = null;
		if (theSource instanceof SupportsNamedGraphs && EmpireUtil.hasNamedGraphSpecified(theObj)) {
			java.net.URI aURI = EmpireUtil.getNamedGraph(theObj);
			if (aURI != null) {
				aNG = aURI.toString();
			}
		}
		boolean aExists = false;
		Dialect aDialect = theSource.getQueryFactory().getDialect();
		final String aUri = aDialect.asQueryString(EmpireUtil.asResource(aKey));
		if (aDialect instanceof SerqlDialect) {
			final String aSeRQL = "select distinct s\n" +
						(aNG == null ? "from\n" : "from context <" + aNG + ">\n") +
						"{s} p {o} where s = " + aUri + " limit 1";
			ResultSet aResults = theSource.selectQuery(aSeRQL);
			try {
				aExists = aResults.hasNext();
			}
			finally {
				aResults.close();
			}
		}
		else {
			// fall back on sparql
			final String aSPARQL = "ask " +
					 (aNG == null ? "" : "from <" + aNG + "> ") +
					 "{ " + aUri + " ?p ?o. }";
			aExists = theSource.ask(aSPARQL);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("{} exists? {}", aUri, Boolean.valueOf(aExists));
		}
		return aExists;
	}

	/**
	 * Return the types of the resource in the data source.
	 * @param theSource the data source
	 * @param theConcept the concept whose type to lookup
	 * @return the rdf:type values for the concept, or null if there is an error or one cannot be found.
	 */
	public static Collection<Resource> getTypes(DataSource theSource, Resource theConcept) {
		Collection<Resource> aTypes = new LinkedList<Resource>();
/*
		if (theSource == null) {
			return null;
		}
*/
		try {
			final Collection<Value> aVals = getValues(theSource, theConcept, RDF.TYPE);
			for (Value v : aVals) {
				aTypes.add((Resource)v);
			}
			// return (Resource) aTypes.iterator().next();
		}
		catch (DataSourceException e) {
			LOGGER.error("There was an error while getting the type of a resource", e);
		}
		LOGGER.debug("Types for <{}>: {}", theConcept, aTypes);
		return aTypes;
	}

	/**
	 * Return the values for the property on the given resource.
	 * @param theSource the data source to query for values
	 * @param theSubject the subject to get property values for
	 * @param thePredicate the property to get values for
	 * @return a collection of all the values of the property on the given resource
	 * @throws com.clarkparsia.empire.ds.DataSourceException if there is an error while querying the data source.
	 */
	public static Collection<Value> getValues(final DataSource theSource, final Resource theSubject, final org.openrdf.model.URI thePredicate) throws DataSourceException {
		ResultSet aResults = null;
		try {
			Dialect aDialect = theSource.getQueryFactory().getDialect();
			String aQuery;
			if (aDialect instanceof SerqlDialect) {
				aQuery = "select obj from {" +
						aDialect.asQueryString(theSubject) + "} <" + thePredicate.stringValue() + "> {obj}";
			}
			else {
				aQuery = "select ?obj where { " +
						aDialect.asQueryString(theSubject) + " <" + thePredicate.stringValue() + "> ?obj. }";
			}
			aResults = theSource.selectQuery(aQuery);
			return Collections2.transform(Sets.newHashSet(aResults), new Function<BindingSet, Value>() {
					public Value apply(final BindingSet theIn) {
						return theIn.getValue("obj");
					}
				});
		}
		catch (Exception e) {
			throw new DataSourceException(e);
		}
		finally {
			if (aResults != null) {
				aResults.close();
			}
		}
	}

	/**
	 * Return the values for the property on the given resource.
	 * @param theSource the data source to query for values
	 * @param theSubject the subject to get property values for
	 * @param thePredicate the property to get values for
	 * @return the first value of the resource
	 * @throws com.clarkparsia.empire.ds.DataSourceException if there is an error while querying the data source.
	 */
	public static Value getValue(final DataSource theSource, final Resource theSubject, final org.openrdf.model.URI thePredicate) throws DataSourceException {
		Collection<Value> aValues = getValues(theSource, theSubject, thePredicate);
		if (aValues.isEmpty()) {
			return null;
		}
		else {
			return aValues.iterator().next();
		}
	}
}
