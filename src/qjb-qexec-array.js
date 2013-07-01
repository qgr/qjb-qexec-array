// Quijibo Query Executor for JavaScript Arrays. Given an Array to be queried,
// this Executor interprets Quijibo Query Trees and returns result rows in
// Arrays.

function expand_meta(meta, array) {
  return _.map(array, function(row) {
    return _.object(meta, row);
  });
}


function execute_query(array_map, qtree) {

  var select = qtree.select;

  var base_array = array_map[select.from];

  var filtered_array = _.filter(base_array, construct_clauses(select.where));

  result = _.map(filtered_array, function(row) {
    return _.pick(row, select.cols);
  });

  return result
}

function construct_clauses(clause) {
  // A clause object may have only one key representing the operation.
  var operator = _.keys(clause)[0];

  var operands = clause[operator];

  if (operator === 'and') {
    return and(_.map(operands, construct_clauses));
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

}


function and(clauses) {
  return function(row) {
    // Apply each clause to each row
    var clause_results = _.map(clauses, function(clause) {
      return clause(row);
    });
    // Every clause must return true
    return  _.every(clause_results, function(b) { return b === true; });
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

