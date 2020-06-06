const sleep = require('./sleep');
var turf = require('@turf/turf');
const THREADS = 800;
const {
  worker,
  worker_substate,
  worker_substates_create,
  worker_collection_insert,
  worker_collection_geo,
  emiter
} = require('./intersectsTools');
let { errores } = require('./intersectsTools');

const TIME_INTERVAL = 1000; // 1000*60
const COLLECTIONS = [
  {
    collection: 'venues',
    idNumber: false
  },
  {
    collection: 'segments',
    idNumber: true
  },
  {
    collection: 'cities',
    idNumber: true
  },
  {
    collection: 'nodes',
    idNumber: true
  },
  {
    collection: 'mapComments',
    idNumber: false
  },
  {
    collection: 'roadClosures',
    idNumber: false
  },
  {
    collection: 'bigJunctions',
    idNumber: true
  },
  {
    collection: 'junctions',
    idNumber: true
  },
  {
    collection: 'managedAreas',
    idNumber: true
  },
  {
    collection: 'mapUpdateRequests',
    idNumber: true
  },
  {
    collection: 'restrictedAreas',
    idNumber: true
  },
  {
    collection: 'problems',
    idNumber: false
  },
  {
    collection: 'gpids',
    idNumber: false
  }
];

async function checkRecrawls(db, io) {
  let processData = false;
  for (let indexCol = 0; indexCol < COLLECTIONS.length; indexCol++) {
    const { collection, idNumber } = COLLECTIONS[indexCol];
    errores = [];
    let count =
      (await db.collection('jsons').countDocuments()) +
      (await db.collection('requests').countDocuments());
    if (count === 0) {
      processData = await worker_collection_insert(
        db,
        collection,
        true,
        true,
        true
      );
      if (processData)
        await worker_collection_geo(io, db, collection, idNumber);
    }
  }
}

const intersects = async (db, io) => {
  while (true) {
    emiter(io);
    const d1 = new Date();

    let processData = false;
    let processDataALL = false;
    for (let indexCol = 0; indexCol < COLLECTIONS.length; indexCol++) {
      const { collection, idNumber } = COLLECTIONS[indexCol];
      errores = [];
      await checkRecrawls(db, io);
      let count =
        (await db.collection('jsons').countDocuments()) +
        (await db.collection('requests').countDocuments());
      if (count === 0) {
        processData = await worker_collection_insert(
          db,
          collection,
          true,
          true
        );
        if (processData)
          await worker_collection_geo(io, db, collection, idNumber);
        processDataALL = processDataALL || processData;
      }
      await sleep(TIME_INTERVAL);
    }
    if (processDataALL) {
      const d2 = new Date();
      console.log(`Intersect FINISH ON ${(d2 - d1) / 1000 / 60} mins`);
    }

    io.sockets.emit('stats', {
      intersectedFinish: true
    });
    await sleep(TIME_INTERVAL);
  }
};
module.exports = intersects;
