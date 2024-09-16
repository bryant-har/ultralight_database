package common;

import java.util.ArrayList;
import java.util.List;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.*;

public class QueryPlanBuilder {
  public QueryPlanBuilder() {
  }

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public Operator buildPlan(Statement stmt) throws NotImplementedException {
    if (!(stmt instanceof Select)) {
      throw new IllegalArgumentException("Only SELECT statements are supported.");
    }

    Select select = (Select) stmt;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    FromItem fromItem = plainSelect.getFromItem();
    Operator root;
    DBCatalog dbCatalog = DBCatalog.getInstance();
    Table table = (Table) fromItem;

    if (fromItem instanceof Table) {
      String tableName = ((Table) fromItem).getName();
      ArrayList<Column> tableColumns = dbCatalog.getColumns(tableName);
      root = new ScanOperator(tableColumns, tableName);

      // Check for ORDER BY clause
      List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
      boolean hasOrderBy = orderByElements != null && !orderByElements.isEmpty();

      // Check for DISTINCT clause
      boolean isDistinct = plainSelect.getDistinct() != null;

      // If DISTINCT is present and there's no ORDER BY, add a SortOperator
      if (isDistinct && !hasOrderBy) {
        // Create a default sort order based on all columns
        orderByElements = new ArrayList<>();
        for (Column col : tableColumns) {
          OrderByElement orderByElement = new OrderByElement();
          orderByElement.setExpression(col);
          orderByElements.add(orderByElement);
        }
        root = new SortOperator(tableColumns, root, orderByElements);
      } else if (hasOrderBy) {
        // If there's an ORDER BY clause, add the SortOperator
        root = new SortOperator(tableColumns, root, orderByElements);
      }

      // Check for AS clause and reset table name appropriately
      tableName = (table.getAlias() != null) ? table.getAlias().getName() : tableName;

      // Check for AS clause for column
      for (SelectItem item : plainSelect.getSelectItems()) {
        if (item instanceof SelectExpressionItem) {
          SelectExpressionItem expressionItem = (SelectExpressionItem) item;
          String newColName = expressionItem.getAlias() != null ? expressionItem.getAlias().getName() : null;
        }

      }

      // If DISTINCT is present, add the DuplicateElementEliminationOperator
      if (isDistinct) {
        root = new DuplicateElementEliminationOperator(tableColumns, root);
      }

      return root;
    } else {
      throw new NotImplementedException("Only single table queries are supported.");
    }
  }
}
