// Quijibo Query Executor for JavaScript Arrays. Given an Array to be queried,
// this Executor interprets Quijibo Query Trees and returns result rows in
// Arrays.

define(function (require) {
  var _ = require('underscore');

  // exports
  return {
    expand_meta: expand_meta,
    execute_query: execute_query,
    construct_clauses: construct_clauses
  };

  function expand_meta(meta, array) {
    return _.map(array, function zip_meta(row) {
      return _.object(meta, row);
    });
  }


  function execute_query(array_map, qtree) {

    var select = qtree.select;

    var result = array_map[select.from];

    if (select.where) {
      result = _.filter(result, construct_clauses(select.where));
    }

    if (select.cols) {
      result = _.map(result, function select_cols(row) {
        return _.pick(row, select.cols);
      });
    }

    // Prototype aggregation.
    if (select.agg) {
      var group_by = select.agg.group_by;
      var grouped = _.groupBy(result, function group(row) {
        return row[group_by];
      });
      console.log(grouped);
      result = _.map(grouped, function(v, k) {
        var val = grouped[k] || 0;
        var agg = {val: sum(pluck_from_groups(val)) };
        agg[group_by] = k;
        return agg;
      });
    }

    if (select.order_by) {
      // Currently only first order_by will be executed.

      var order_by = select.order_by[0]

      var col = order_by[0];

      var type = order_by[1];

      result = _.sortBy(result, col);

      if (type === 'desc') {
        result.reverse();
      }
    }

    if (select.limit) {
      result = result.slice(0, select.limit)
    }

    return result
  }

  function construct_clauses(clause) {
    // A clause object may have only one key representing the operation.
    var operator = _.keys(clause)[0];

    var operands = clause[operator];

    if (operator === 'and') {
      return and(_.map(operands, construct_clauses));
    }
    if (operator === 'or') {
      return or(_.map(operands, construct_clauses));
    }
    if (operator === 'eq') {
      return construct_op(eq, operands)
    }
    if (operator === 'ne') {
      return construct_op(ne, operands)
    }
    if (operator === 'lt') {
      return construct_op(lt, operands)
    }
    if (operator === 'lte') {
      return construct_op(lte, operands)
    }
    if (operator === 'gt') {
      return construct_op(gt, operands)
    }
    if (operator === 'gte') {
      return construct_op(gte, operands)
    }
    if (operator === 'in') {
      var col = operands[0];
      var in_list = operands[1];
      return function(row) {
        return _.contains(in_list, row[col]);
      }
    }

  }

  function and(clauses) {
    return function(row) {
      // Apply each clause to each row
      var clause_results = _.map(clauses, function map_clauses(clause) {
        return clause(row);
      });
      // Every clause must return true
      return  _.every(clause_results);
    }
  }

  function or(clauses) {
    return function(row) {
      // Apply each clause to each row
      var clause_results = _.map(clauses, function map_clauses(clause) {
        return clause(row);
      });
      // Every clause must return true
      return  _.some(clause_results);
    }
  }

  function construct_op(op, operands) {
    // Currently, only operators with two operands are supported.
    var col = operands[0];
    var other = operands[1];
    return function(row) {
      return op(row[col], other)
    }
  }

  // Functional versions of comparators
  function eq(a, b) {
    return a === b;
  }

  function ne(a, b) {
    return a !== b;
  }

  function lt(a, b) {
    return a < b;
  }

  function lte(a, b) {
    return a <= b;
  }

  function gt(a, b) {
    return a > b;
  }

  function gte(a, b) {
    return a >= b;
  }

  function pluck_from_groups(arr) {
    return _.map(arr, function(m) { return m.val; });
  };

  function sum(arr) {
    return _.reduce(arr, function(memo, num) { return memo + num; }, 0);
  };


});

