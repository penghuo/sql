package com.amazon.opendistroforelasticsearch.sql.expression.function;

import com.amazon.opendistroforelasticsearch.sql.common.utils.StringUtils;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
import com.amazon.opendistroforelasticsearch.sql.data.type.WideningTypeRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
  private final ParamType paramType;

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
    this.paramType = new ParamType(
        paramTypeList.stream().map(ParamType.InternalType::single).collect(Collectors.toList()));
  }

  public static FunctionSignature var(FunctionName functionName, ExprType type, ExprType varType) {
    return new FunctionSignature(functionName,
        new ParamType(Arrays.asList(ParamType.InternalType.single(type),
            ParamType.InternalType.var(varType))));
  }

  public static FunctionSignature var(FunctionName functionName, ExprType varType) {
    return new FunctionSignature(functionName,
        new ParamType(Collections.singletonList(ParamType.InternalType.var(varType))));
  }

//  /**
//   * calculate the function signature match degree.
//   *
//   * @return EXACTLY_MATCH: exactly match
//   * NOT_MATCH: not match
//   * By widening rule, the small number means better match
//   */
//  public int match(FunctionSignature functionSignature) {
//    List<ExprType> functionTypeList = functionSignature.getParamTypeList();
//    if (!functionName.equals(functionSignature.getFunctionName())
//        || paramTypeList.size() != functionTypeList.size()) {
//      return NOT_MATCH;
//    }
//
//    int matchDegree = EXACTLY_MATCH;
//    for (int i = 0; i < paramTypeList.size(); i++) {
//      ExprType paramType = paramTypeList.get(i);
//      ExprType funcType = functionTypeList.get(i);
//      int match = WideningTypeRule.distance(paramType, funcType);
//      if (match == WideningTypeRule.IMPOSSIBLE_WIDENING) {
//        return NOT_MATCH;
//      } else {
//        matchDegree += match;
//      }
//    }
//    return matchDegree;
//  }

  /**
   * calculate the function signature match degree.
   *
   * @param name       the function name.
   * @param unResolvedParamTypes the list of parameter types.
   * @return EXACTLY_MATCH: exactly match
   * NOT_MATCH: not match
   * By widening rule, the small number means better match
   */
  public int match(FunctionName name, List<ExprType> unResolvedParamTypes) {
    if (!functionName.equals(name)) {
      return NOT_MATCH;
    }
    return paramType.match(unResolvedParamTypes);
  }

  /**
   * util function for formatted arguments list.
   */
  public String formatTypes() {
    return paramType.toString();
  }

  @RequiredArgsConstructor
  public static class ParamType {

    private final Iterator<InternalType> thisTypeIter;

    private final String formattedType;

    private InternalType currentType = null;

    public ParamType(List<InternalType> types) {
      thisTypeIter = types.iterator();
      this.formattedType = types.stream()
          .map(InternalType::toString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    public int match(List<ExprType> otherTypes) {
      int matchDegree = EXACTLY_MATCH;
      for (int i = 0; i < otherTypes.size(); i++) {
        Optional<ExprType> thisExprTypeOptional = nextType();
        if (thisExprTypeOptional.isPresent()) {
          int match = WideningTypeRule.distance(thisExprTypeOptional.get(), otherTypes.get(i));
          if (match == WideningTypeRule.IMPOSSIBLE_WIDENING) {
            return NOT_MATCH;
          } else {
            matchDegree += match;
          }
        } else {
          return NOT_MATCH;
        }
      }

      if (currentType.isSingleType() && currentType.hasNext()) {
        return NOT_MATCH;
      }
      return matchDegree;
    }

    private Optional<ExprType> nextType() {
      if (currentType != null && currentType.hasNext()) {
        return Optional.of(currentType.next());
      } else {
        if (thisTypeIter.hasNext()) {
          currentType = thisTypeIter.next();
          return nextType();
        }
        return Optional.empty();
      }
    }

    @Override
    public String toString() {
      return formattedType;
    }

    @AllArgsConstructor
    public static class InternalType implements Iterator<ExprType> {
      private final ExprType type;
      private final boolean alwaysHasNext;
      private boolean toggle;

      public static InternalType single(ExprType type) {
        return new InternalType(type, false, true);
      }

      public static InternalType var(ExprType type) {
        return new InternalType(type, true, true);
      }

      public boolean isSingleType() {
        return !alwaysHasNext;
      }

      @Override
      public boolean hasNext() {
        if (alwaysHasNext) {
          return true;
        } else {
          if (toggle) {
            toggle = false;
            return true;
          }
          return false;
        }
      }

      @Override
      public ExprType next() {
        return type;
      }

      @Override
      public String toString() {
        if (alwaysHasNext) {
          return StringUtils.format("VAR(%s)", type.typeName());
        } else {
          return type.typeName();
        }
      }
    }
  }
}
