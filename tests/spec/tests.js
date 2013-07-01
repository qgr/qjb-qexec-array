var projects_raw = {
  meta: [
    'name',
    'start_year',
    'type',
    'cost'
  ],
  array: [
    ['ACID TEST III', 1970, 'FIELDEX', 10000000],
    ['ARCTIC CANDY', 1969, 'OPORD', 50000000],
    ['BACK PORCH', 1965, 'CONSTRUCTION', 600000],
    ['BERNIE', 1989, 'INTEL', 15000000],
    ['BIG BELLY', 1965, 'EQUIPMENT', 500000],
    ['BIG BIRD', 1970, 'EQUIPMENT', 6000000],
    ['BRILLIANT PEBBLES', 1982, 'EQUIPMENT', 15000000],
    ['BUBBLE DANCER', 1992, 'OPORD', 45000000],
    ['BURNING CANDY', 1968, 'OPORD', 30000000],
    ['CLARINET ANCHOR', 1955, 'INTEL', 35000000],
    ['CLASSIC WIZARD', 1976, 'CONSTRUCTION', 55000000],
    ['CLUSTER GIRL', 1977, 'INTEL', 85000000],
    ['COBRA TIME', 1988, 'INTEL', 45000000],
    ['COMFY DRESS', 1966, 'INTEL', 1100000],
    ['CUTTY SHARK', 1972, 'EQUIPMENT', 75000000],
    ['DIAMOND DUST', 1960, 'RESEARCH', 850000000],
    ['DIDO QUEEN', 1973, 'FIELDEX', 2000000],
    ['HARVEST PEOPLE', 2014, 'OPORD', 95000000000],
    ['HEAD DANCER', 1961, 'EQUIPMENT', 3500000],
    ['IVORY JUSTICE', 1988, 'FIELDEX', 65000000],
    ['LINEAR UNICORN', 1967, 'OPORD', 90000000],
    ['LOOKING GLASS', 1985, 'EQUIPMENT', 3000000],
    ['NYMPH VOICE', 1983, 'RESEARCH', 150000000],
    ['PEACE ZEBRA', 1971, 'EQUIPMENT', 3000000],
    ['WHITE CLOUD', 1966, 'EQUIPMENT', 55000000]
  ]
}

var projects = expand_meta(projects_raw.meta, projects_raw.array);

qtree = {
  select: {
    cols: [
      'name',
      'cost'
    ],
    from: projects,
    where: {
      and: [
        {
          eq: [
          'type',
          'INTEL'
          ]
        },
        {
          gte: [
          'start_year',
          1960
          ]
        }
      ]
    }
  }
}

describe("Construct query", function() {

  it("returns query results",
    function() {
      var expected = [
  { name : 'BERNIE', cost : 15000000 },
  { name : 'CLUSTER GIRL', cost : 85000000 },
  { name : 'COBRA TIME', cost : 45000000 },
  { name : 'COMFY DRESS', cost : 1100000 }
  ]

      expect(execute_query(projects, qtree))
      .toEqual(expected);
  });

});

