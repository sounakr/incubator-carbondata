package org.apache.carbondata.core.scan.expression;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;

import static org.apache.carbondata.core.scan.filter.intf.ExpressionType.AND;

/**
 * Created by root1 on 10/3/17.
 */
public class ExpressionContainer {
  private Expression expr;

  public ExpressionContainer(Expression expr) {
    this.expr = expr;
  }

  public Expression getExpr() { return expr;}

  public void setExpr (Expression expr) {
    this.expr = expr;
  }

  public void rangeTreeTraversal(Expression node) {
    // Just traverse the tree
    Expression rootNode;
    if (null == node) {
      // This is the beginning.
      rootNode = getExpr();
    }
    else {
      rootNode = node;
    }

    // print the node
    System.out.println(rootNode.getString());

    if (null != rootNode && rootNode.getChildren().size() > 0) {
      for (Expression exp : rootNode.getChildren()) {
        rangeTreeTraversal(exp);
      }
    }
  }


  public void rangeExpressionEvaluator (Expression currentNode, Expression parentNode) {
    // Phase 1
    // Traverse through the tree in preorder traversal
    // In case we found a node which
    // a) AND Operator.
    // b) One of its child is less than and greater than nodes
    // c) The less than and greater than nodes should have same column expression
    // d) Other node should be a literal expression. and those literal should form a range.

    // Then this node qualifies for range expression conversion.
    /*
                AND                                          Range
                 |                                             |
                / \                                           /  \
               /   \                                         /    \
            Less   Greater         =>                    Column  RangeLiteral
             / \    / \                                    |       / \
            /   \  /   \                                   a      /   \
           a    10 a   5                                       Less   greater
                                                                 |      |
                                                                 10     5
     */


    if (null == currentNode) {
      currentNode = expr;
      parentNode = expr;
    }

    if (null != currentNode) {
      if (null != currentNode.getFilterExpressionType() &&
          currentNode.getFilterExpressionType().equals(AND) &&
          currentNode.getChildren().size() == 2) {
        // Check the nodes if they are less than and greater than.
        boolean lessVal = false;
        boolean greaterVal = false;
        String columnA = null;
        String columnB = null;
        Expression lessExpression = null;
        Expression greaterExpression = null;
        DataType dataTypeA = null;
        DataType dataTypeB = null;

        for (Expression exp : currentNode.getChildren()) {
          if ((exp instanceof LessThanEqualToExpression) ||
              (exp instanceof LessThanExpression)) {
            lessVal = true;
            lessExpression = exp;
            for(Expression lessExpChild : exp.getChildren()) {
              if (lessExpChild instanceof ColumnExpression) {
                columnA = ((ColumnExpression) lessExpChild).getColumnName();
              }
              if (lessExpChild instanceof LiteralExpression) {
                dataTypeA = ((LiteralExpression) lessExpChild).getLiteralExpDataType();
              }
            }
          }
          if ((exp instanceof GreaterThanEqualToExpression) ||
              (exp instanceof GreaterThanExpression)) {
            greaterVal = true;
            greaterExpression = exp;

            for(Expression greaterExpChild : exp.getChildren()) {
              if (greaterExpChild instanceof ColumnExpression) {
                columnB = ((ColumnExpression) greaterExpChild).getColumnName();
              }
              if (greaterExpChild instanceof LiteralExpression) {
                dataTypeB = ((LiteralExpression) greaterExpChild).getLiteralExpDataType();
              }
            }
          }
        }
        if ((lessVal == greaterVal == true) && (null != columnA && columnA == columnB) && (
            null != dataTypeA && (dataTypeA == dataTypeB))) {
          // Create the range Tree.
          RangeExpression rangeTree = new RangeExpression(lessExpression, greaterExpression);
          if (currentNode.equals(expr)) {
            this.setExpr(rangeTree);
          } else {
            parentNode.setChildren(currentNode, rangeTree);
          }
        }
      }
      for (Expression nextExp : currentNode.getChildren()) {
           if (null != nextExp) {
             rangeExpressionEvaluator(nextExp, currentNode);
           }
      }
    }
  }
}
