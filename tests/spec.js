
define(function(require) {

  var qexec = require('src/qjb-qexec-array');

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

  var projects = qexec.expand_meta(projects_raw.meta, projects_raw.array);

  var projects_small_raw = {
    meta: [
      'name',
      'start_year',
      'type',
      'cost'
    ],
    array: [
      ['LINEAR UNICORN', 1967, 'OPORD', 90000000],
      ['PEACE ZEBRA', 1971, 'EQUIPMENT', 3000000],
      ['NYMPH VOICE', 1983, 'EQUIPMENT', 150000000],
    ]
  }

  var projects_small = qexec.expand_meta(
      projects_small_raw.meta,
      projects_small_raw.array);

  var array_map = {
    projects: projects,
    projects_small: projects_small
  }

  describe("execute_query", function() {


    it("can execute unqualified query",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects_small',
          }
        }

        var expected = [
          { name : 'LINEAR UNICORN'},
          { name : 'PEACE ZEBRA'},
          { name : 'NYMPH VOICE'}
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute limit query",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            limit: 2
          }
        }

        var expected = [
          { name : 'ACID TEST III' },
          { name : 'ARCTIC CANDY' }
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute eq clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            where: {
              eq: [
                'name',
                'LINEAR UNICORN'
              ]
            }
          }
        }

        var expected = [
          { name : 'LINEAR UNICORN'},
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute ne clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects_small',
            where: {
              ne: [
                'name',
                'LINEAR UNICORN'
              ]
            }
          }
        }

        var expected = [
          { name : 'PEACE ZEBRA'},
          { name : 'NYMPH VOICE'},
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute lt clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            where: {
              lt: [
                'start_year',
                1960
              ]
            }
          }
        }

        var expected = [
          { name : 'CLARINET ANCHOR'},
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute lte clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            where: {
              lte: [
                'start_year',
                1960
              ]
            }
          }
        }

        var expected = [
          { name : 'CLARINET ANCHOR'},
          { name : 'DIAMOND DUST'}
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });


    it("can execute gt clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            where: {
              gt: [
                'start_year',
                1992
              ]
            }
          }
        }

        var expected = [
          { name : 'HARVEST PEOPLE'},
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute gte clause",
      function() {

         var qtree = {
          select: {
            cols: ['name'],
            from: 'projects',
            where: {
              gte: [
                'start_year',
                1992
              ]
            }
          }
        }

        var expected = [
          { name : 'BUBBLE DANCER' },
          { name : 'HARVEST PEOPLE'},
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });


    it("can execute and queries",
      function() {

         var qtree = {
          select: {
            cols: [
              'name',
              'cost'
            ],
            from: 'projects',
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
        var expected = [
          { name : 'BERNIE', cost : 15000000 },
          { name : 'CLUSTER GIRL', cost : 85000000 },
          { name : 'COBRA TIME', cost : 45000000 },
          { name : 'COMFY DRESS', cost : 1100000 }
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute or queries",
      function() {

         var qtree = {
          select: {
            cols: [
              'name',
              'cost'
            ],
            from: 'projects',
            where: {
              or: [
                {
                  eq: [
                  'start_year',
                  1977
                  ]
                },
                {
                  eq: [
                  'start_year',
                  1966
                  ]
                }
              ]
            }
          }
        }
        var expected = [
          { name : 'CLUSTER GIRL', cost : 85000000 },
          { name : 'COMFY DRESS', cost : 1100000 },
          { name : 'WHITE CLOUD', cost : 55000000 }
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute asc order by",
      function() {

         var qtree = {
          select: {
            cols: ['name', 'cost'],
            from: 'projects_small',
            order_by: [
              ['cost', 'asc']
            ]
          }
        }

        var expected = [
          { name : 'PEACE ZEBRA', cost : 3000000 },
          { name : 'LINEAR UNICORN', cost : 90000000 },
          { name : 'NYMPH VOICE', cost : 150000000 }
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    it("can execute desc order by",
      function() {

         var qtree = {
          select: {
            cols: ['name', 'cost'],
            from: 'projects_small',
            order_by: [
              ['cost', 'desc']
            ]
          }
        }

        var expected = [
          { name : 'NYMPH VOICE', cost : 150000000 },
          { name : 'LINEAR UNICORN', cost : 90000000 },
          { name : 'PEACE ZEBRA', cost : 3000000 }
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });

    // Disabled until implemented.
    /*
    it("can execute combined asc and desc order by",
      function() {

         var qtree = {
          select: {
            cols: ['name', 'cost', 'type'],
            from: 'projects_small',
            order_by: [
              ['type', 'desc'],
              ['cost', 'asc']
            ]
          }
        }

        var expected = [
          { name : 'LINEAR UNICORN', cost : 90000000, type : 'OPORD' },
          { name : 'PEACE ZEBRA', cost : 3000000, type : 'EQUIPMENT' },
          { name : 'NYMPH VOICE', cost : 150000000, type : 'EQUIPMENT' },
        ]

        expect(qexec.execute_query(array_map, qtree))
        .toEqual(expected);
    });
    */

  });
});
