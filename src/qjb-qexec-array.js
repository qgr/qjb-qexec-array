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

    var agg_func_map = {
      'sum': sum
    };

    // Prototype aggregation.
    if (select.agg) {

      var group_by_list = select.agg.group_by;

      var grouped;
      if (group_by_list) {
        grouped = group_arr(result, group_by_list);
      } else {
        grouped = group_arr(result);
      }

      var func = agg_func_map[select.agg.func];
      var col = select.agg.col;

      result = aggregate_on_groups(grouped, func, col);

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
      var offset = select.offset || 0;
      result = result.slice(offset, select.limit)
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
    if (operator === 'match') {
      var col = operands[0];
      var match_regex = new RegExp(operands[1]);
      return function(row) {
        if (row[col].match(match_regex)) {
          return true;
        }
      }
    }
    if (operator === 'imatch') {
      var col = operands[0];
      var match_regex = new RegExp(operands[1], 'i');
      return function(row) {
        if (row[col].match(match_regex)) {
          return true;
        }
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

  function pluck_from_groups(arr, col) {
    return _.map(arr, function(m) { return m[col]; });
  };

  function sum(arr) {
    return _.reduce(arr, function(memo, num) { return memo + num; }, 0);
  };

  function group_arr(arr, group_by_list) {
    var grouped = _.groupBy(arr, function group(row) {
      // We build a group key, which is an object where the keys
      // are the columns to be grouped on, and point to the values
      // of those columns in the group.
      var group_key = {};
      _.each(group_by_list, function(group_by) {
        group_key[group_by] = row[group_by];
      });

      // We then stringify the object so we can use it as a key in an object.
      // This is a hack to enable an easy group-key-to-rows map.
      return JSON.stringify(group_key);
    });
    return grouped;
  }

  function aggregate_on_groups(grouped, func, col) {
      return _.map(grouped, function(v, k) {
        var val = grouped[k] || 0;
        var agg = {};
        agg[col] = func(pluck_from_groups(val, col));

        // We deserialize group-key after aggregating.
        var group_key = JSON.parse(k);
        agg = _.extend(agg, group_key);

        return agg;
      });
  }


});

