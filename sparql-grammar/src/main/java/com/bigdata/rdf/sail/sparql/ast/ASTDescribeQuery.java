/* Generated By:JJTree: Do not edit this line. ASTDescribeQuery.java */

package com.bigdata.rdf.sail.sparql.ast;

import com.bigdata.rdf.sail.sparql.ast.ASTDescribe;
import com.bigdata.rdf.sail.sparql.ast.ASTQuery;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilder;
import com.bigdata.rdf.sail.sparql.ast.SyntaxTreeBuilderVisitor;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;

public class ASTDescribeQuery extends ASTQuery {

	public ASTDescribeQuery(int id) {
		super(id);
	}

	public ASTDescribeQuery(SyntaxTreeBuilder p, int id) {
		super(p, id);
	}

	@Override
	public Object jjtAccept(SyntaxTreeBuilderVisitor visitor, Object data)
		throws VisitorException
	{
		return visitor.visit(this, data);
	}
	
	public ASTDescribe getDescribe() {
		return jjtGetChild(ASTDescribe.class);
	}
}