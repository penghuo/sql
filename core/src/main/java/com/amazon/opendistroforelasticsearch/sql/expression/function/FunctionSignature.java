package com.amazon.opendistroforelasticsearch.sql.expression.function;

import com.amazon.opendistroforelasticsearch.sql.common.utils.StringUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.type.WideningTypeRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Function signature is composed by function name and arguments list.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
public class FunctionSignature {
  public static final Integer NOT_MATCH = Integer.MAX_VALUE;
  public static final Integer EXACTLY_MATCH = 0;

  private final FunctionName functionName;
  private final SignatureType signatureType;

  /**
   * The default constructor which build the fixed size parameter list.
   *
   * @param functionName  function name.
   * @param paramTypeList fixed size parameter list
   */
  public FunctionSignature(
      FunctionName functionName,
      List<ExprType> paramTypeList) {
    this.functionName = functionName;
    this.signatureType = new SignatureType(
        paramTypeList.stream().map(SignatureType.InternalType::single).collect(Collectors.toList()));
  }

  /**
   * Todo.
   */
  public static FunctionSignature var(FunctionName functionName, ExprType type, ExprType varType) {
    return new FunctionSignature(functionName,
        new SignatureType(Arrays.asList(SignatureType.InternalType.single(type),
            SignatureType.InternalType.var(varType))));
  }

  /**
   * Todo.
   */
  public static FunctionSignature var(FunctionName functionName, ExprType varType) {
    return new FunctionSignature(functionName,
        new SignatureType(Collections.singletonList(SignatureType.InternalType.var(varType))));
  }

  /**
   * calculate the function signature match degree.
   *
   * @param name       the function name.
   * @param argumentTypes the list of parameter types.
   * @return EXACTLY_MATCH: exactly match
   *         NOT_MATCH: not match
   *         By widening rule, the small number means better match
   */
  public int match(FunctionName name, List<ExprType> argumentTypes) {
    if (!functionName.equals(name) || signatureType.size() > argumentTypes.size()) {
      return NOT_MATCH;
    }

    int matchDegree = EXACTLY_MATCH;
    for (int i = 0; i < argumentTypes.size(); i++) {
      final Optional<ExprType> optionalSignatureType = signatureType.get(i);
      if (optionalSignatureType.isPresent()) {
        int match = WideningTypeRule.distance(argumentTypes.get(i), optionalSignatureType.get());
        if (match == WideningTypeRule.IMPOSSIBLE_WIDENING) {
          return NOT_MATCH;
        } else {
          matchDegree += match;
        }
      } else {
        return NOT_MATCH;
      }
    }
    return matchDegree;
  }

  /**
   * util function for formatted arguments list.
   */
  public String formatTypes() {
    return signatureType.toString();
  }

  @RequiredArgsConstructor
  public static class SignatureType {

    private final List<InternalType> types;

    private final String formattedType;

    private final Integer size;

    /**
     * Todo.
     */
    public SignatureType(List<InternalType> types) {
      this.types = types;
      this.formattedType = types.stream()
          .map(InternalType::toString)
          .collect(Collectors.joining(",", "[", "]"));
      this.size = types.size();
    }

    public int size() {
      return size;
    }

    public Optional<ExprType> get(int index) {
      if (types.size() == 0) {
        return Optional.empty();
      }
      if (index >= types.size()) {
        InternalType lastType = types.get(types.size() - 1);
        if (lastType.isVarType()) {
          return Optional.of(lastType.getType());
        } else {
          return Optional.empty();
        }
      } else {
        return Optional.of(types.get(index).getType());
      }
    }

    @Override
    public String toString() {
      return formattedType;
    }

    @AllArgsConstructor
    public static class InternalType {
      @Getter
      private final ExprType type;
      private final boolean varType;

      public static InternalType single(ExprType type) {
        return new InternalType(type, false);
      }

      public static InternalType var(ExprType type) {
        return new InternalType(type, true);
      }

      public boolean isVarType() {
        return varType;
      }

      @Override
      public String toString() {
        if (varType) {
          return StringUtils.format("VAR(%s)", type.typeName());
        } else {
          return type.typeName();
        }
      }
    }
  }
}
