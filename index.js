var ostrich = require('ostrich-bindings');
var _ = require('lodash');

var generate = 5;
var minCardinality = 5000;
var limit = 10;
var path = '/Users/rtaelman/nosync/OSTRICH/ostrich/run-bear-b/';

var queries = {};

ostrich.fromPath(path, true, (error, store) => {
  if (error) throw error;
  store.countTriplesDeltaMaterialized(null, null, null, 0, store.maxVersion, (error, totalCount, hasExactCount) => {
    getCardinalityTriplePattern(store, totalCount, minCardinality).then((query) => {
      pushQueries(store, totalCount);
    });
  });
});

function pushQueries(store, totalCount) {
  getCardinalityTriplePattern(store, totalCount, minCardinality).then((query) => {
    queries[JSON.stringify(query)] = query;
    if (_.values(queries).length < generate) {
      pushQueries(store, totalCount);
    } else {
      console.log(queries); // TODO
      _.values(queries).forEach((query) => {
        var offset = 2;
        while (offset < query.cardinality) {
          console.log("%s %s %s %d %d", query.triplePattern.subject || '?s', query.triplePattern.predicate || '?p', query.triplePattern.object || '?o', offset, limit);
          offset *= 2;
        }
      });
      store.close();
    }
  });
}

function getTriple(store, totalCount) {
  return new Promise((resolve, reject) => {
    let index = Math.floor(Math.random() * totalCount);
    store.searchTriplesDeltaMaterialized(null, null, null,
      { offset: index, limit: 1, versionStart: 0, versionEnd: store.maxVersion }, (error, triple) => {
        if (error) reject(error);
        resolve(triple[0]);
      });
  });
}

function getTriplePattern(store, totalCount) {
  return getTriple(store, totalCount).then((triple) => {
    let component = Math.floor(Math.random() * 3);
    if (component === 0) {
      return { subject: triple.subject };
    } else if (component === 1) {
      return { predicate: triple.predicate };
    } else {
      return { object: triple.object };
    }
  });
}

function getCount(store, triplePattern) {
  return new Promise((resolve, reject) => {
      store.countTriplesVersionMaterialized(triplePattern.subject, triplePattern.predicate, triplePattern.object, store.maxVersion, (error, totalCount, hasExactCount) => {
        if (error) reject(error);
        resolve(totalCount);
      });
  });
}

function getCardinalityTriplePattern(store, totalCount, expectedCount) {
  return getTriplePattern(store, totalCount).then((triplePattern) => {
    return getCount(store, triplePattern).then((count) => {
      if (count > expectedCount) {
        return { cardinality: count, triplePattern: triplePattern };
      } else {
        return getCardinalityTriplePattern(store, totalCount, expectedCount);
      }
    })
  });
}