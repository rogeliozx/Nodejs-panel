const ss = require('socket.io-stream');
const turf = require('@turf/turf');
const ObjectID = require('mongodb').ObjectID;
const parser = require('cron-parser');
const {
  worker_collection_insert,
  worker_collection_geo,
  emiter
} = require('./crawler/intersectsTools');
const THREADS_TILES = 50;
const { Pool, Client } = require('pg');
const pool = new Pool({
  user: 'waze',
  host: 'localhost',
  database: 'panelz',
  password: 'waze',
  port: 5432
});

const TILESTHREADS = 20;
const COLLECTIONS_INTERSECT = [
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
function setupIO(io) {
  async function intersect(poly1, poly2) {
    try {
      const intersect = turf.intersect(poly1, poly2);
      if (intersect) {
        return { intersect, tile: poly2 };
      } else {
        return null;
      }
    } catch (error) {
      try {
        const p1 = turf.intersect(poly1, poly1);
        let intersect = null;
        for (let index = 0; index < p1.geometry.geometries.length; index++) {
          const e = p1.geometry.geometries[index];
          try {
            let inter = turf.intersect(e, poly2);
            if (inter !== null) {
              intersect = inter;
            }
            // console.log(intersect)
          } catch (error2) {
            console.log(error2);
          }
        }
        if (intersect !== null) {
          return { intersect, tile: poly2 };
        } else {
          return null;
        }
      } catch (error) {
        console.log(error);
        return null;
      }
    }
  }
  function generateSquares(bbox, zoom = 4) {
    const valZoom = 0.2 / zoom;
    const _minX = parseFloat(
      (Math.floor(bbox[0] / valZoom) * valZoom).toFixed(2)
    );
    const _minY = parseFloat(
      (Math.floor(bbox[1] / valZoom) * valZoom).toFixed(2)
    );
    const _maxX = parseFloat(
      (Math.floor(bbox[2] / valZoom) * valZoom + valZoom).toFixed(2)
    );
    const _maxY = parseFloat(
      (Math.floor(bbox[3] / valZoom) * valZoom + valZoom).toFixed(2)
    );
    var polygons = [];
    var squares = [];
    for (let indexY = _minY; indexY <= _maxY; indexY += valZoom) {
      for (let indexX = _minX; indexX <= _maxX; indexX += valZoom) {
        const minX = parseFloat(indexX.toFixed(2));
        const minY = parseFloat(indexY.toFixed(2));
        const maxX = parseFloat((indexX + valZoom).toFixed(2));
        const maxY = parseFloat((indexY + valZoom).toFixed(2));

        var polygon = turf.polygon([
          [
            [minX, minY],
            [maxX, minY],
            [maxX, maxY],
            [minX, maxY],
            [minX, minY]
          ]
        ]);
        polygons.push(polygon);
        squares.push({
          minX,
          minY,
          maxX,
          maxY
        });
      }
    }
    var collection = turf.featureCollection(polygons);
    return { collection, squares };
  }

  async function parseTiles(
    geoObj,
    socket,
    tileName,
    properties = {},
    ALLTILES = {}
  ) {
    //   console.log(`Type ${geoObj.type}`)
    try {
      switch (geoObj.type) {
        case 'FeatureCollection':
          for (
            let featureI = 0;
            featureI < geoObj.features.length;
            featureI++
          ) {
            const feature = geoObj.features[featureI];
            await parseTiles(feature, socket, tileName, properties, ALLTILES);
          }
          // geoObj.features.forEach(feature => {

          // })
          break;
        case 'Feature':
          await parseTiles(
            geoObj.geometry,
            socket,
            tileName,
            geoObj.properties,
            ALLTILES
          );

          break;
        case 'GeometryCollection':
          for (
            let geometryI = 0;
            geometryI < geoObj.geometries.length;
            geometryI++
          ) {
            const geometry = geoObj.geometries[geometryI];
            await parseTiles(geometry, socket, tileName, properties, ALLTILES);
          }
          break;

        case 'MultiPolygon':
          for (let multiI = 0; multiI < geoObj.coordinates.length; multiI++) {
            const multy = geoObj.coordinates[multiI];
            const polygon = turf.polygon(multy);
            polygon.properties = properties;
            await parseTiles(polygon, socket, tileName, properties, ALLTILES);
          }
          break;
        case 'Polygon':
          if (properties.Substate) {
            properties.SubState = properties.Substate;
            delete properties.Substate;
          }
          const polygon = geoObj;
          var bbox = turf.bbox(geoObj);

          var { collection, squares } = generateSquares(bbox);
          var len = collection.features.length;
          var totalTiles = 0;
          var newFeatures = [];

          var bulkTiles = [];
          var bulkState = [];
          var bulkSubState = [];
          var bulkCountry = [];
          for (let squareI = 0; squareI < len; squareI += THREADS_TILES) {
            // console.log(`leyendo tile ${squareI} de ${len}`)
            // const square = collection.features[squareI];
            // var intersection = turf.intersect(geoObj, square);
            // if (intersection) {
            //     newFeatures.push(square)
            //     totalTiles += 1
            // }
            let threads = [];
            for (let t = squareI; t < squareI + THREADS_TILES && t < len; t++) {
              const prom = intersect(geoObj, collection.features[t]);
              // const prom = intersect(geoObj,geoObj);
              threads.push(prom);
            }
            let values = await Promise.all(threads);
            for (let i = 0; i < values.length; i++) {
              const obj = values[i];
              const objSquare = squares[squareI + i];
              if (obj) {
                const _minX = objSquare.minX;
                const _minY = objSquare.minY;
                const _maxX = objSquare.maxX;
                const _maxY = objSquare.maxY;

                const Tile = {
                  north: _maxY,
                  south: _minY,
                  east: _maxX,
                  west: _minX
                };
                const substateId = `${properties.Country},${properties.State},${properties.SubState}`;
                const intersectGeo = {
                  substate: substateId,
                  intersect: obj.intersect,

                  country: properties.Country,
                  state: properties.State,
                  sstate: properties.SubState
                };

                bulkTiles.push({
                  updateOne: {
                    filter: {
                      _id: `${Tile.west},${Tile.south},${Tile.east},${Tile.north}`
                    },
                    update: {
                      $set: Tile,
                      $addToSet: {
                        properties: { ...properties, id: substateId },
                        Env: properties.Env,
                        intersect: intersectGeo
                      }
                    },
                    upsert: true
                  }
                });

                bulkCountry.push({
                  updateOne: {
                    filter: {
                      Country: properties.Country,
                      Env: properties.Env
                    },
                    update: {
                      $set: { Country: properties.Country, Env: properties.Env }
                    },
                    upsert: true
                  }
                });

                bulkState.push({
                  updateOne: {
                    filter: { State: properties.State, Env: properties.Env },
                    update: {
                      $set: {
                        Country: properties.Country,
                        State: properties.State,
                        Env: properties.Env
                      }
                    },
                    upsert: true
                  }
                });

                bulkSubState.push({
                  updateOne: {
                    filter: { _id: substateId },
                    update: {
                      $set: properties,
                      $addToSet: {
                        intersect: obj.intersect,
                        polygons: polygon
                      }
                    },
                    upsert: true
                  }
                });
                if (
                  ALLTILES[
                    `${Tile.north},${Tile.south},${Tile.east},${Tile.west}`
                  ] === undefined
                ) {
                  ALLTILES[
                    `${Tile.north},${Tile.south},${Tile.east},${Tile.west}`
                  ] = Tile;
                  totalTiles += 1;
                }
                newFeatures.push(obj.intersect);
              }
            }
          }
          if (bulkTiles.length > 0) {
            await io.db.collection('tiles').bulkWrite(bulkTiles);
          }
          if (bulkCountry.length > 0) {
            await io.db.collection('country').bulkWrite(bulkCountry);
          }
          if (bulkState.length > 0) {
            await io.db.collection('state').bulkWrite(bulkState);
          }
          if (bulkSubState.length > 0) {
            await io.db.collection('substate').bulkWrite(bulkSubState);
          }
          // newFeatures.push(geoObj)
          socket.emit('uploading', tileName, null, totalTiles, newFeatures);
          break;
        case 'Point':
          var bbox = turf.bbox(geoObj);
          var totalTiles = 0;
          var { collection, squares } = generateSquares(bbox);
          var len = collection.features.length;
          for (let squareI = 0; squareI < len; squareI++) {
            // console.log(`leyendo tile ${squareI} de ${len}`)
            const square = collection.features[squareI];
            // newFeatures.push(square)
            // totalTiles += 1
          }
          //   newFeatures.push(geoObj)
          socket.emit('uploading', tileName, null, totalTiles);
          break;

        default:
          console.log('default', geoObj);
          break;
      }
    } catch (error) {
      console.log(error);
      // parseTiles(geoObj, socket, tileName, properties)
    }
  }

  const tileWorker = (collection, squares, postgres, zoomValue) => async (
    initial,
    final
  ) => {
    for (let indexTile = initial; indexTile < final; indexTile++) {
      try {
        const f = collection.features[indexTile];
        const objSquare = squares[indexTile];
        if (objSquare) {
          const _minX = objSquare.minX;
          const _minY = objSquare.minY;
          const _maxX = objSquare.maxX;
          const _maxY = objSquare.maxY;

          const Tile = {
            north: _maxY,
            south: _minY,
            east: _maxX,
            west: _minX
          };
          const tileID = `${zoomValue}__${Tile.west},${Tile.south},${Tile.east},${Tile.north}`;

          const queryTile = {
            text:
              'INSERT INTO tiles (name,geom, west, south, east, north, zoom) ' +
              'VALUES ($1,ST_SetSRID(ST_GeomFromGeoJSON($2), 4326),$3, $4, $5, $6, $7) ' +
              'ON CONFLICT (name) DO ' +
              'UPDATE SET geom = ST_SetSRID(ST_GeomFromGeoJSON($2), 4326)',
            values: [
              tileID,
              f.geometry,
              Tile.west,
              Tile.south,
              Tile.east,
              Tile.north,
              zoomValue
            ]
          };
          await postgres.query(queryTile);
        }
      } catch (error) {
        console.log(error);
      }
    }
  };

  async function parseTilesPOSTGIS(
    geoObj,
    socket,
    tileName,
    properties = {},
    ALLTILES = {}
  ) {
    //   console.log(`Type ${geoObj.type}`)
    try {
      switch (geoObj.type) {
        case 'FeatureCollection':
          for (
            let featureI = 0;
            featureI < geoObj.features.length;
            featureI++
          ) {
            const feature = geoObj.features[featureI];
            await parseTilesPOSTGIS(
              feature,
              socket,
              tileName,
              properties,
              ALLTILES
            );
          }
          // geoObj.features.forEach(feature => {

          // })
          break;
        case 'Feature':
          await parseTilesPOSTGIS(
            geoObj.geometry,
            socket,
            tileName,
            geoObj.properties,
            ALLTILES
          );

          break;
        case 'Point':
        case 'GeometryCollection':
        case 'MultiPolygon':
        case 'Polygon':
          if (properties.Substate) {
            properties.SubState = properties.Substate;
            delete properties.Substate;
          }
          const polygon = geoObj;
          var bbox = turf.bbox(geoObj);

          var totalTiles = 0;
          var newFeatures = [];

          var bulkTiles = [];
          var bulkState = [];
          var bulkSubState = [];
          var bulkCountry = [];
          bulkCountry.push({
            updateOne: {
              filter: { Country: properties.Country, Env: properties.Env },
              update: {
                $set: { Country: properties.Country, Env: properties.Env }
              },
              upsert: true
            }
          });

          bulkState.push({
            updateOne: {
              filter: { State: properties.State, Env: properties.Env },
              update: {
                $set: {
                  Country: properties.Country,
                  State: properties.State,
                  Env: properties.Env
                }
              },
              upsert: true
            }
          });

          properties.substateID = `${properties.substateID}.${properties.Country}`;
          bulkSubState.push({
            updateOne: {
              filter: { _id: properties.substateID },
              update: {
                $set: properties
              },
              upsert: true
            }
          });

          // if (bulkTiles.length > 0) {
          //     await io.db.collection('tiles').bulkWrite( bulkTiles);
          // }
          if (bulkCountry.length > 0) {
            await io.db.collection('country').bulkWrite(bulkCountry);
          }
          if (bulkState.length > 0) {
            await io.db.collection('state').bulkWrite(bulkState);
          }
          if (bulkSubState.length > 0) {
            await io.db.collection('substate').bulkWrite(bulkSubState);
          }

          const postgres = await pool.connect();
          const query = {
            text:
              'INSERT INTO substates (name,geom, substateid, stateid, countryid, substate,state,country, Env) ' +
              'VALUES ($1,ST_SetSRID(ST_GeomFromGeoJSON($2), 4326), $3, $4, $5, $6, $7, $8, $9) ' +
              'ON CONFLICT (name) DO ' +
              'UPDATE SET geom = ST_SetSRID(ST_GeomFromGeoJSON($2), 4326)',
            values: [
              properties.substateID,
              geoObj,
              properties.substateID,
              properties.stateID,
              properties.countryID,
              properties.SubState,
              properties.State,
              properties.Country,
              properties.Env
            ]
          };
          await postgres.query(query);

          // zoom4
          let zoomValue = 4;
          let { collection, squares } = generateSquares(bbox, zoomValue);
          let len = collection.features.length;

          let tamano = Math.ceil(collection.features.length / TILESTHREADS);
          let jobsTiles = [];
          for (let iT = 0; iT < collection.features.length; iT = iT + tamano) {
            jobsTiles.push(
              tileWorker(
                collection,
                squares,
                postgres,
                zoomValue
              )(iT, iT + tamano)
            );
          }
          await Promise.all(jobsTiles);

          // zoom2
          zoomValue = 2;
          ({ collection, squares } = generateSquares(bbox, zoomValue));
          tamano = Math.ceil(collection.features.length / TILESTHREADS);
          jobsTiles = [];
          for (let iT = 0; iT < collection.features.length; iT = iT + tamano) {
            jobsTiles.push(
              tileWorker(
                collection,
                squares,
                postgres,
                zoomValue
              )(iT, iT + tamano)
            );
          }
          await Promise.all(jobsTiles);

          // zoom2
          zoomValue = 1;
          ({ collection, squares } = generateSquares(bbox, zoomValue));
          tamano = Math.ceil(collection.features.length / TILESTHREADS);
          jobsTiles = [];
          for (let iT = 0; iT < collection.features.length; iT = iT + tamano) {
            jobsTiles.push(
              tileWorker(
                collection,
                squares,
                postgres,
                zoomValue
              )(iT, iT + tamano)
            );
          }
          await Promise.all(jobsTiles);

          postgres.release();

          socket.emit('uploading', tileName, null, len, newFeatures);
          break;

        default:
          console.log('default', geoObj);
          break;
      }
    } catch (error) {
      console.log(error);
      // parseTiles(geoObj, socket, tileName, properties)
    }
  }
  async function parseTilesProcess(geoObj, socket, tileName) {
    const d1 = new Date();
    console.log(`Empezo ${tileName}`);

    const postgres = await pool.connect();
    await postgres.query(`CREATE TABLE IF NOT EXISTS substates (
            id serial PRIMARY KEY,
            name VARCHAR UNIQUE,
            substateid VARCHAR,
            stateid VARCHAR,
            countryid VARCHAR,
            substate VARCHAR,
            Env VARCHAR,
            state VARCHAR,
            country VARCHAR,
            geom geometry(Geometry,4326)
        );`);
    await postgres.query(`
            CREATE INDEX IF NOT EXISTS geom_substates
            ON public.substates USING gist(geom);`);

    await postgres.query(`CREATE TABLE IF NOT EXISTS tiles (
            id serial PRIMARY KEY,
            name VARCHAR UNIQUE,
            west FLOAT,
            south FLOAT,
            east FLOAT,
            north FLOAT,
            zoom integer,
            geom geometry(Geometry,4326)
        );`);
    await postgres.query(`
            CREATE INDEX IF NOT EXISTS geom_tiles
            ON public.tiles USING gist(geom);`);

    await parseTilesPOSTGIS(geoObj, socket, tileName);

    try {
      const tiempo = new Date();
      const tableName = `TEMP_${tiempo.getTime()}`;
      await postgres.query(`
                DROP TABLE IF EXISTS ${tableName};`);
      await postgres.query(`
                CREATE TABLE ${tableName} (
                    id serial PRIMARY KEY,
                    name VARCHAR,
                    substateid VARCHAR,
                    stateid VARCHAR,
                    countryid VARCHAR,
                    Env VARCHAR,
                    west FLOAT,
                    south FLOAT,
                    east FLOAT,
                    north FLOAT,
                    zoom integer,
                    geom geometry(Geometry,4326)
                );`);
      await postgres.query(`
                INSERT INTO ${tableName}(name,geom, west, south, east, north, substateid, stateid, countryid, Env, zoom)
                SELECT t.name, t.geom, t.west, t.south, t.east, t.north, s.substateid, s.stateid, s.countryid, s.Env, t.zoom
                    FROM public.tiles t 
                    JOIN public.substates s
                    ON ST_Intersects(t.geom,s.geom)`);
      const query = ` SELECT name,geom, west, south, east, north, substateid, stateid, countryid, env, zoom FROM ${tableName};`;
      const result = await postgres.query(query);
      const bulk = [];
      for (let ir = 0; ir < result.rows.length; ir++) {
        const row = result.rows[ir];
        // console.log(row)
        bulk.push({
          updateOne: {
            filter: { _id: row.name.split('__')[1] },
            update: {
              $set: {
                west: row.west,
                south: row.south,
                east: row.east,
                north: row.north,
                zoom: parseInt(row.name.split('__')[0])
              },
              $addToSet: {
                substate: row.substateid,
                Env: row.env
              }
            },
            upsert: true
          }
        });
      }
      if (bulk.length > 0) {
        const res = await io.db.collection('tiles').bulkWrite(bulk);
        console.log(res);
      }
    } catch (error) {
      console.log(error);
    }

    postgres.release();
    const d2 = new Date();

    console.log(`Termino ${tileName} ${(d2 - d1) / 1000 / 60} mins`);
  }

  io.on('connection', function(socket) {
    ss(socket).on('file', function(stream, data, callback) {
      console.log(data);
      var size = 0;
      var geojson = '';
      stream.on('data', chunk => {
        geojson += chunk.toString();
        size += chunk.length;
        // console.log(Math.floor(size / data.size * 100) + '%');
        socket.emit(
          'uploading',
          data.name,
          Math.floor((size / data.size) * 100),
          0
        );
      });
      stream.on('end', () => {
        console.log('There will be no more data.');
        const mapObj = JSON.parse(geojson);
        console.log(mapObj);
        parseTilesProcess(mapObj, socket, data.name);
      });
    });

    console.log('Connected!');
    socket.emit('news', { hello: 'world' });
    socket.on('my other event', function(data) {
      console.log(data);
    });

    socket.on('getcountries', async function(cb) {
      var countries = await io.db
        .collection('country')
        .find()
        .sort({ Country: 1 })
        .toArray();
      cb(countries);
    });
    socket.on('getstates', async function(Country, cb) {
      var states = await io.db
        .collection('state')
        .find({ Country })
        .sort({ State: 1 })
        .toArray();
      cb(states);
    });

    const intersectLoadings = [];
    socket.on('countIntersects', async function(collection, cb) {
      var countC = 0;
      var countCI = 0;
      if (collection === 'substate') {
        // const aggregate = await io.db.collection(collection).aggregate([
        //     {
        //         $project: {
        //             intersects: {
        //                 $cond: {
        //                     if: {
        //                         $isArray: "$intersect"
        //                     }, then: {
        //                         $size: "$intersect"
        //                     }, else: 0
        //                 }
        //             }
        //         }
        //     },
        //     {
        //     $group:
        //         {
        //             _id:"intersects",
        //             count: { $sum: "$intersects"  }
        //         }
        //     }
        // ]).toArray()
        countC = await io.db
          .collection(collection)
          .find()
          .count();
        const postgres = await pool.connect();
        const countRes = await postgres.query(
          `SELECT count(*) FROM public.substates;`
        ); //
        const { 0: { count = 0 } = {} } = countRes.rows;
        countCI = parseInt(count);
        postgres.release();
      } else if (collection === 'gpids') {
        countC = await io.db
          .collection(collection)
          .find()
          .count();
        countCI = await io.db
          .collection(collection)
          .find()
          .count();
      } else {
        countC = await io.db
          .collection(collection)
          .find({ geometry: { $exists: true } })
          .count();
        countCI = await io.db
          .collection(collection)
          .find({ geometry: { $exists: true }, _geoSubStateId: { $ne: null } })
          .count();
      }
      cb({
        collection,
        countC,
        countCI,
        intersectLoadings
      });
    });
    socket.on('createIntersects', async function(collection, cb) {
      const col = COLLECTIONS_INTERSECT.find(c => c.collection === collection);

      processData = await worker_collection_insert(
        io.db,
        col.collection,
        true,
        true
      );
      if (processData)
        await worker_collection_geo(io, io.db, col.collection, col.idNumber);

      var countC = 0;
      var countCI = 0;
      if (collection === 'substate') {
        countC = await io.db
          .collection(collection)
          .find()
          .count();
        const postgres = await pool.connect();
        const countRes = await postgres.query(
          `SELECT count(*) FROM public.substates;`
        ); //
        const { 0: { count = 0 } = {} } = countRes.rows;
        countCI = parseInt(count);
        postgres.release();
      } else if (collection === 'gpids') {
        countC = await io.db
          .collection(collection)
          .find()
          .count();
        countCI = await io.db
          .collection(collection)
          .find()
          .count();
      } else {
        countC = await io.db
          .collection(collection)
          .find({ geometry: { $exists: true } })
          .count();
        countCI = await io.db
          .collection(collection)
          .find({ geometry: { $exists: true }, _geoSubStateId: { $ne: null } })
          .count();
      }
      cb({
        collection,
        countC,
        countCI,
        intersectLoadings
      });
    });

    socket.on('getsubstates', async function(Country, State, cb) {
      var states = await io.db
        .collection('substate')
        .find({ Country, State })
        .sort({ SubState: 1 })
        .toArray();
      // var intersects = []

      // for (let index = 0; index < states.length; index++) {
      //     const state = states[index];
      //     intersects = intersects.concat(state.intersect)
      // }

      // for (let index = 0; index < states.length; index++) {
      //     const state = states[index];
      //     state.intersect = intersects
      // }
      cb(states);
    });
    socket.on('gettiles', async function(SubState, State, cb) {
      const properties = { substate: SubState };
      var states = await io.db
        .collection('tiles')
        .find(properties)
        .toArray();
      cb(states);
    });
    socket.on('savecrawler', async function(
      {
        crawlerName,
        crawlerHours,
        substatesTags,
        mapcomments,
        urs,
        venues,
        placeupdates,
        segments,
        managedareas,
        roadclosures,
        zoom,
        crawlId
      },
      cb
    ) {
      // if (placeupdates) {
      //     mapcomments = false
      //     urs = false
      //     venues = false
      //     segments = false
      //     managedareas = false
      //     roadclosures = false
      // }
      var substates = [];
      for (let index = 0; index < Object.keys(substatesTags).length; index++) {
        const sub = Object.keys(substatesTags)[index];
        substates.push(substatesTags[sub]._id);
      }
      let run = new Date();
      try {
        if (
          typeof crawlerHours === 'string' ||
          crawlerHours instanceof String
        ) {
          if (crawlerHours.trim() !== '0') {
            var interval = parser.parseExpression(crawlerHours);
            run = interval.next().toDate();
          }
        }
      } catch (error) {}

      if (crawlId === null) {
        var crawl = await io.db
          .collection('crawls')
          .insertOne({
            crawlerName,
            crawlerHours,
            substates,
            run,
            mapcomments,
            urs,
            venues,
            placeupdates,
            segments,
            managedareas,
            roadclosures,
            zoom
          });
        cb(crawl);
      } else {
        var crawl = await io.db.collection('crawls').updateOne(
          { _id: new ObjectID(crawlId) },
          {
            $set: {
              crawlerName,
              crawlerHours,
              substates,
              run,
              mapcomments,
              urs,
              venues,
              placeupdates,
              segments,
              managedareas,
              roadclosures,
              zoom
            }
          }
        );
        cb(crawl);
      }
    });

    socket.on('startcrawls', async function(crawlid, cb) {
      await io.db
        .collection('crawls')
        .updateOne(
          { _id: new ObjectID(crawlid) },
          { $set: { activate: true } }
        );
      cb();
    });
    socket.on('removeCrawl', async function(crawlid, cb) {
      await io.db.collection('crawls').remove({ _id: new ObjectID(crawlid) });
      cb();
    });
    socket.on('getcrawl', async function(crawlid, cb) {
      try {
        const crawl = await io.db
          .collection('crawls')
          .findOne({ _id: new ObjectID(crawlid) });
        const { substates: sIds = [] } = crawl;

        const substates = await io.db
          .collection('substate')
          .find({ _id: { $in: sIds } })
          .toArray();
        const substatesTags = substates.reduce((past, curr) => {
          past[`${curr.Country}-${curr.State}-${curr.SubState}`] = curr;
          return past;
        }, {});

        const statesIds = substates.map(s => s.State);
        const countryIds = substates.map(s => s.Country);
        const states = await io.db
          .collection('state')
          .find({ State: { $in: statesIds } })
          .toArray();
        // const countries = await io.db.collection("country").find({Country:{$in:countryIds}}).toArray()

        cb({
          ...crawl,
          substates,
          states,
          substatesTags
          // countries
        });
      } catch (error) {
        cb();
      }
    });

    socket.on('getcrawls', async function(cb) {
      var crawls = await io.db
        .collection('crawls')
        .find()
        .sort({ crawlerName: 1 })
        .toArray();
      cb(crawls);
    });
    socket.on('recrawlTile', async function({ tileId }, cb) {
      var tiles = await io.db
        .collection('tiles')
        .find({ _id: tileId })
        .toArray();
      // console.log(tiles.length)
      for (let tilesIndex = 0; tilesIndex < tiles.length; tilesIndex++) {
        const tile = tiles[tilesIndex];
        for (let EnvIndex = 0; EnvIndex < tile.Env.length; EnvIndex++) {
          const Env = tile.Env[EnvIndex];
          const request = {
            Env,
            east: tile.east,
            west: tile.west,
            south: tile.south,
            north: tile.north,
            tile: tile._id,
            substate: 'recrawl',
            mapcomments: true,
            urs: true,
            venues: true,
            placeupdates: true,
            segments: true,
            managedareas: true,
            roadclosures: true
          };
          await io.db.collection('requests').insertOne(request);
        }
      }
      cb('requests created');
    });

    // socket.on('getcrawls', function (cb) {

    //     io.db.collection('crawls').aggregate([
    //         {
    //           $lookup:
    //             {
    //               from: "substate",
    //               localField: "substates",
    //               foreignField: "_id",
    //               as: "substates_docs"
    //             }
    //        }
    //      ])
    //     .toArray(async function (err,result) {
    //         if (err) {
    //             console.error(err)
    //             cb({err});
    //         }else{
    //             cb({crawls:result});
    //         }
    //     });
    // });

    socket.on('addFile', function(file) {
      const obj = JSON.parse(file.toString('utf8'));
    });
  });
}
module.exports = setupIO;
