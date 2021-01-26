/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.analysis;

import com.amazon.opendistroforelasticsearch.sql.analysis.symbol.Namespace;
import com.amazon.opendistroforelasticsearch.sql.analysis.symbol.Symbol;
import com.amazon.opendistroforelasticsearch.sql.analysis.symbol.SymbolTable;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.exception.SemanticCheckException;
import com.amazon.opendistroforelasticsearch.sql.expression.Expression;
import com.amazon.opendistroforelasticsearch.sql.expression.ReferenceExpression;
import com.amazon.opendistroforelasticsearch.sql.expression.env.Environment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The definition of Type Environment.
 */
public class TypeEnvironment implements Environment<Symbol, ExprType> {
  @Getter
  private final TypeEnvironment parent;

  // default symbol table.
  private final SymbolTable symbolTable;

  // Map between index name and symbol table.
  private final Map<String, SymbolTable> indices = new HashMap<>();

  // Map between variable name and the tuple of index name and field name.
  private final Map<String, Pair<String, String>> variables = new HashMap<>();

  public TypeEnvironment(TypeEnvironment parent) {
    this.parent = parent;
    this.symbolTable = new SymbolTable();
  }

  public TypeEnvironment(TypeEnvironment parent, SymbolTable symbolTable) {
    this.parent = parent;
    this.symbolTable = symbolTable;
  }

  /**
   * Resolve the {@link Expression} from environment.
   *
   * @param symbol Symbol
   * @return resolved {@link ExprType}
   */
  @Override
  public ExprType resolve(Symbol symbol) {
    return resolve(symbolTable, symbol);
  }

  /**
   * Resolve all fields in the current environment.
   * @param namespace     a namespace
   * @return              all symbols in the namespace
   */
  public Map<String, ExprType> lookupAllFields(Namespace namespace) {
    Map<String, ExprType> result = new LinkedHashMap<>();
    symbolTable.lookupAllFields(namespace).forEach(result::putIfAbsent);
    return result;
  }

  /**
   * Define symbol with the type.
   *
   * @param symbol symbol to define
   * @param type   type
   */
  public void define(Symbol symbol, ExprType type) {
    symbolTable.store(symbol, type);
  }

  public void define(String index, Map<String, ExprType> fieldTypes) {
    SymbolTable indexSymbolTable = new SymbolTable();
    fieldTypes.forEach((k, v) -> {
      final Symbol symbol = new Symbol(Namespace.FIELD_NAME, k);
      indexSymbolTable.store(symbol, v);
      symbolTable.store(symbol, v);
    });
    indices.putIfAbsent(index, indexSymbolTable);
  }

  /**
   * Define expression with the type.
   *
   * @param ref {@link ReferenceExpression}
   */
  public void define(ReferenceExpression ref) {
    define(new Symbol(Namespace.FIELD_NAME, ref.getAttr()), ref.type());
  }

  public void remove(Symbol symbol) {
    symbolTable.remove(symbol);
  }

  /**
   * Remove ref.
   */
  public void remove(ReferenceExpression ref) {
    remove(new Symbol(Namespace.FIELD_NAME, ref.getAttr()));
  }

  // If there is no qualified id, the default table will be used.
  public String defaultIndex() {
    List<String> indicesList = new ArrayList<>(indices.keySet());
    if (indicesList.size() == 1) {
      return indicesList.get(0);
    } else {
      throw new RuntimeException(String.format("failed to get default index form indices: %s",
          indicesList));
    }
  }

  public boolean isIndex(String indexName) {
    return indices.containsKey(indexName);
  }

  public boolean isVariable(String varName) {
    return variables.containsKey(varName);
  }

  /**
   * Resolve the fieldName in the specified index
   */
  public ExprType resolve(String fieldName) {
    return resolve(new Symbol(Namespace.FIELD_NAME, fieldName));
  }

  /**
   * Resolve the fieldName in the specified index
   */
  public ExprType resolveInIndex(String index, String fieldName) {
    return resolve(indicesSymbolTable(index), new Symbol(Namespace.FIELD_NAME, fieldName));
  }

  /**
   * Resolve the variable to the index name and field symbol
   */
  public ExprType resolveInVariable(String var, String fieldName) {
    if (variables.containsKey(var)) {
      final Pair<String, String> tuple = variables.get(var);
      return resolveInIndex(tuple.getLeft(), String.join(".", tuple.getRight(), fieldName));
    } else {
      throw new SemanticCheckException(
          String.format("can't find symbol table of variable: %s", var));
    }
  }

  private ExprType resolve(SymbolTable symbolTable, Symbol symbol) {
    Optional<ExprType> typeOptional = symbolTable.lookup(symbol);
    if (typeOptional.isPresent()) {
      return typeOptional.get();
    }
    throw new SemanticCheckException(String.format("can't resolve %s in type env", symbol));
  }

  private SymbolTable indicesSymbolTable(String index) {
    if (indices.containsKey(index)) {
      return indices.get(index);
    } else {
      throw new SemanticCheckException(
          String.format("can't find symbol table of index: %s", index));
    }
  }
}
