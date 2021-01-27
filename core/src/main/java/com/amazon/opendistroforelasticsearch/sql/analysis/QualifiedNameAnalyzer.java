package com.amazon.opendistroforelasticsearch.sql.analysis;

import com.amazon.opendistroforelasticsearch.sql.ast.expression.QualifiedName;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import java.util.Collections;
import java.util.Optional;

/**
 * Todo.
 */
public class QualifiedNameAnalyzer {

  private TypeEnvironment env;

  public QualifiedNameAnalyzer(AnalysisContext context) {
    this.env = context.peek();
  }

  /**
   * Todo.
   */
  public ReferenceExpression resolve(QualifiedName qualifiedName) {
    final Optional<String> sourceId = qualifiedName.first();

    if (sourceId.isPresent()) {
      String source = sourceId.get();
      // e.addr
      if (env.isIndex(source)) {
        final QualifiedName rest = qualifiedName.rest();
        return new ReferenceExpression(source, rest.getParts(), env.resolveInIndex(source,
            rest.toString()));
      } else if (env.isVariable(source)) { // p.addr
        final QualifiedName rest = qualifiedName.rest();
        return new ReferenceExpression(source, rest.getParts(), env.resolveInVariable(source,
            rest.toString()));
      }
    }
    // addr / addr.state
    final String fieldName = qualifiedName.toString();
    return new ReferenceExpression(env.defaultIndex(), qualifiedName.getParts(),
        env.resolve(fieldName));
  }

  public ReferenceExpression resolve(String fullName) {
    return new ReferenceExpression(env.defaultIndex(), Collections.singletonList(fullName),
        env.resolve(fullName));
  }
}
