const { Pool, Client } = require('pg');
const pool = new Pool({
  user: 'waze',
  host: 'localhost',
  database: 'panelz',
  password: 'waze',
  port: 5432
});
let errores = [];
let intersected = 0;
async function worker(db, cursor, query, props, geo = 'geometry') {
  let total = 0;
  try {
    const postgres = await pool.connect();
    while (await cursor.hasNext()) {
      const obj = await cursor.next();
      const geometry = obj[geo];
      const geoJSON = JSON.stringify(geometry);
      const values = [geoJSON];
      for (let i = 0; i < props.length; i++) {
        const prop = props[i];
        values.push(obj[prop]);
      }
      const exec = {
        text: query,
        values: values
      };
      if (geometry) {
        await postgres.query(exec);
      }
      total++;
    }
    postgres.release();
  } catch (error) {
    errores.push(error.message);
    console.log(error.message);
    try {
      postgres.release();
    } catch (error) {}
  }
  return total;
}

async function worker_substate(db, cursor) {
  let total = 0;
  try {
    const postgres = await pool.connect();
    while (await cursor.hasNext()) {
      const tile = await cursor.next();

      for (let index = 0; index < tile.intersect.length; index++) {
        const polygon = tile.intersect[index];
        const geo = JSON.stringify(polygon.intersect.geometry);
        const query = {
          text:
            'INSERT INTO substates (name,geom, tile, substateid,substate,state,country) ' +
            'VALUES ($1,ST_SetSRID(ST_GeomFromGeoJSON($2), 4326), $3, $4, $5, $6, $7) ' +
            'ON CONFLICT (name) DO ' +
            'UPDATE SET geom = ST_SetSRID(ST_GeomFromGeoJSON($2), 4326)',
          values: [
            `${tile._id}-${polygon.substate}-${index}`,
            geo,
            tile._id,
            polygon.substate,
            polygon.sstate,
            polygon.state,
            polygon.country
          ]
        };
        await postgres.query(query);
        total++;
      }
    }
    postgres.release();
  } catch (error) {
    errores.push(error.message);
    console.log(error.message);
    try {
      postgres.release();
    } catch (error) {}
  }
  return total;
}

async function worker_substates_create(db, drop = false) {
  const postgres = await pool.connect();
  if (drop) {
    await postgres.query(`DROP TABLE IF EXISTS substates;`);
    await postgres.query(`
        CREATE TABLE substates (
          id serial PRIMARY KEY,
          name VARCHAR UNIQUE,
          tile VARCHAR,
          substateid VARCHAR,
          stateid VARCHAR,
          countryid VARCHAR,
          substate VARCHAR,
          state VARCHAR,
          country VARCHAR,
          geom geometry(Geometry,4326)
      );`);
    await postgres.query(`
        CREATE INDEX geom_substates
        ON public.substates USING gist(geom);`);
  }
  const cursor = await db.collection('tiles').find();

  const workers = [];
  const count = await cursor.count();
  const THREADS = 10;
  for (let index = 0; index < THREADS; index++) {
    const limit = count / THREADS;
    const cursor = await db
      .collection('tiles')
      .find()
      .sort({ _id: 1 })
      .skip(limit * index)
      .limit(limit + 1);
    workers.push(worker_substate(db, cursor));
  }

  const d1 = new Date();
  const v = await Promise.all(workers);

  postgres.release();
  const d2 = new Date();
  console.log(v.reduce((past, curret) => past + curret));
  console.log(v);
  console.log(errores);
  console.log(`${(d2 - d1) / 1000 / 60} mins`);
}

async function worker_collection_insert(
  db,
  collection,
  drop = false,
  check_geoSubStateId = false,
  justRecrawl = false
) {
  const postgres = await pool.connect();
  let query = {};
  if (check_geoSubStateId) {
    query = { geometry: { $exists: true }, _processed: { $ne: true } };
  } else {
    query = { geometry: { $exists: true } };
  }
  if (justRecrawl) {
    query.recrawl = { $gt: 0 };
  }
  if (drop) {
    await postgres.query(`DROP TABLE IF EXISTS waze_${collection};`);
    await postgres.query(`DROP INDEX IF EXISTS geom_collection_${collection};`);
    await postgres.query(`
        CREATE TABLE waze_${collection} (
          id serial PRIMARY KEY,
          name VARCHAR UNIQUE,
          geom geometry(Geometry,4326)
      );`);
    await postgres.query(`
        CREATE INDEX geom_collection_${collection}
        ON public.waze_${collection} USING gist(geom);`);
  }

  const workers = [];
  const cursor = await db.collection(collection).find(query);
  const count = await cursor.count();
  let processData = false;

  const d1 = new Date();
  if (count > 0) {
    processData = true;

    const THREADS = 10;
    for (let index = 0; index < THREADS; index++) {
      const limit = count / THREADS;
      const insert =
        `INSERT INTO waze_${collection} (name,geom) ` +
        'VALUES ($2,ST_SetSRID(ST_GeomFromGeoJSON($1), 4326)) ' +
        'ON CONFLICT (name) DO ' +
        'UPDATE SET geom = ST_SetSRID(ST_GeomFromGeoJSON($1), 4326)';
      const values = ['_id'];
      const cursor = await db
        .collection(collection)
        .find(query)
        .sort({ _id: 1 })
        .skip(limit * index)
        .limit(limit + 1);
      workers.push(worker(db, cursor, insert, values));
    }

    const v = await Promise.all(workers);
    console.log(v.reduce((past, curret) => past + curret));
    console.log(v);
    console.log(errores);

    await db.collection(collection).updateMany(query, {
      $set: {
        _processed: true
      }
    });
    await postgres.query(`
          DROP TABLE IF EXISTS joined_waze_${collection};`);
    await postgres.query(`
          CREATE TABLE joined_waze_${collection} (
            id serial PRIMARY KEY,
            name VARCHAR,
            geom geometry(Geometry,4326),
            substateid VARCHAR,
            stateid VARCHAR,
            countryid VARCHAR,
            substate VARCHAR,
            state VARCHAR,
            country VARCHAR
          );`);
    await postgres.query(`
          INSERT INTO joined_waze_${collection}(name, geom, substateid, stateid, countryid, substate, state, country)
          SELECT v.name, v.geom, s.substateid, s.stateid, s.countryid, s.substate, s.state, s.country
            FROM public.waze_${collection} v 
            JOIN public.substates s
            ON ST_Intersects(v.geom,s.geom);`);

    const d2 = new Date();

    console.log(`${(d2 - d1) / 1000} segs`);
  }

  postgres.release();
  return processData;
}

async function emiter(io) {
  io.sockets.emit('stats', {
    intersected
  });
}
async function worker_collection_geo(
  io,
  db,
  collection,
  idNumber = false,
  batch = 100000
) {
  const d1 = new Date();

  const postgres = await pool.connect();
  const countRes = await postgres.query(
    ` SELECT COUNT(*) FROM joined_waze_${collection} `
  ); //
  const { 0: { count = 0 } = {} } = countRes.rows;
  console.log(`${count} ${collection} to add`);
  let steps = 0;
  while (steps * batch < count) {
    const query = ` SELECT name, geom, substateid, stateid, countryid, substate, state, country FROM joined_waze_${collection} order by id limit ${batch} offset ${steps *
      batch}`;
    const result = await postgres.query(query); //
    const bulk = [];
        for (let ir = 0; ir < result.rows.length; ir++) {
      const row = result.rows[ir];
      intersected = intersected + 1;
      io.sockets.emit('stats', {
        intersected,
        intersectedFinish: false
      });
      bulk.push({
        updateOne: {
          filter: { _id: idNumber ? parseInt(row.name) : row.name },
          update: {
            $set: {
              _geoSubStateId: row.substateid,
              _geoStateId: row.stateid,
              _geoCuntryId: row.countryid,
              _geoSubState: row.substate,
              _geoState: row.state,
              _geoCountry: row.country
            }
          },
          upsert: false
        }
      });
    }

    steps = steps + 1;
    if (bulk.length > 0) {
      const res = await db.collection(collection).bulkWrite(bulk);
      console.log(`${steps * batch} ${collection} updated`);
    }
  }

  // const queryCity = `SELECT substateid, substate, state, country, COUNT(*) as count
  //       FROM joined_waze_${collection}
  //       GROUP BY substateid, substate, state, country;`
  // // console.log(query)
  // let  resultCity = await postgres.query(queryCity) //
  let resultCity = await db
    .collection(collection)
    .aggregate([
      {
        $group: {
          _id: '$_geoSubStateId',
          count: { $sum: 1 }
        }
      }
    ])
    .toArray();
  const bulkCity = [];
  // console.log(resultCity)
  for (let ir = 0; ir < resultCity.length; ir++) {
    const row = resultCity[ir];
    // console.log(row)
    bulkCity.push({
      updateOne: {
        filter: { _id: row._id },
        update: {
          $set: {
            [collection]: row.count
          }
        },
        upsert: false
      }
    });
  }

  if (bulkCity.length > 0) {
    const res = await db.collection('substate').bulkWrite(bulkCity);
    // console.log(`${steps*batch} ${collection} on`)
    // console.log(res)
  }

  postgres.release();
  const d2 = new Date();

  console.log(`${(d2 - d1) / 1000} segs`);
}

var exports = (module.exports = {});

exports.errores = errores;
exports.worker = worker;
exports.worker_substate = worker_substate;
exports.worker_substates_create = worker_substates_create;
exports.worker_collection_insert = worker_collection_insert;
exports.worker_collection_geo = worker_collection_geo;
exports.emiter = emiter;
