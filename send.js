var MongoClient = require('mongodb').MongoClient	;

var database = "panelz";
var url = "mongodb://localhost:27017/"+database+"?maxPoolSize=434";

const { Pool, Client } = require('pg')
const pool = new Pool({
  user: 'waze',
  host: 'localhost',
  database: 'wze',
  password: 'waze',
  port: 5432,
})
const pool_panelz = new Pool({
  user: 'waze',
  host: 'localhost',
  database: 'panelz',
  password: 'waze',
  port: 5432,
})
MongoClient.connect(url,{ useNewUrlParser: true, useUnifiedTopology: true }, async function(err, dbase) {
  if (err) throw err;
  try {
    var db = dbase.db(database);
    let errores = []
    async function worker(cursor, query, props, geo='geometry') {
      let total = 0
      try { 
        const postgres = await pool.connect()
        while (await cursor.hasNext()) {
          const obj = await cursor.next();
          const geometry = obj[geo]
          const geoJSON = JSON.stringify(geometry)
          const values = [geoJSON]
          for (let i = 0; i < props.length; i++) {
            const prop = props[i];
            values.push(obj[prop])
          }
          values.push(JSON.stringify(obj))
          const exec = {
            text: query,
            values: values,
          }
          await postgres.query(exec)
          // console.log(`${venue._id} Added`)
          total++
        }
        postgres.release()
      } catch (error) {
        errores.push(error)
        console.log(error)
        try {
          postgres.release()
        } catch (error) {}
      }
      return total
    }
    async function worker_collection_insert(collection, drop=false) {
        errores = []
        const d1 = new Date()
        const postgres = await pool.connect()
        await postgres.query(`DROP TABLE IF EXISTS waze_${collection};`)
        await postgres.query(`DROP INDEX IF EXISTS geom_collection_${collection};`)
        await postgres.query(`
            CREATE TABLE waze_${collection} (
                id serial PRIMARY KEY,
                name VARCHAR UNIQUE,
                geom geometry(Geometry,4326),
                data json
            );`)
        await postgres.query(`
            CREATE INDEX geom_collection_${collection}
            ON public.waze_${collection} USING gist(geom);`)

        // **************     ADD INDEX ON <COLLECTION>
        if (collection === 'segments') {

          await postgres.query(`DROP TABLE IF EXISTS waze_road_type;`)
          await postgres.query(`CREATE TABLE "waze_road_type" (roadType integer PRIMARY KEY, roadName varchar(30) NOT NULL, roadFamily varchar(30) NOT NULL);`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('1', 'Local Street', 'Local public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('2', 'Primary Street', 'Local public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('3', 'Freeway (Interstate / Other)', 'Major public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('4', 'Ramp', 'Major public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('5', 'Routable Pedestrian Path', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('6', 'Major Highway', 'Major public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('7', 'Minor Highway', 'Major public roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('8', 'Off-road / Not maintained', 'Other drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('9', 'Walkway', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('10', 'Non-Routable Pedestrian Path', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('15', 'Ferry', 'Other drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('16', 'Stairway', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('17', 'Private Road', 'Other drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('18', 'Railroad', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('19', 'Runway/Taxiway', 'Non-drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('20', 'Parking Lot Road', 'Other drivable roads');`)
          await postgres.query(`INSERT INTO "waze_road_type" VALUES ('22', 'Passageway', 'Local public roads');`)
          await postgres.query(`CREATE INDEX waze_roadtypedx ON "waze_road_type" (roadType);`)
          await postgres.query(`CREATE INDEX waze_roadnamedx ON "waze_road_type" (roadName);`)
          await postgres.query(`CREATE INDEX waze_roadfamilydx ON "waze_road_type" (roadFamily);`)

          await postgres.query(`CREATE INDEX waze_segments_fwdToll_btree ON "waze_segments" USING BTREE ((data->>'fwdToll'));`)
          await postgres.query(`CREATE INDEX waze_segments_revToll_btree ON "waze_segments" USING BTREE ((data->>'revToll'));`)
          await postgres.query(`CREATE INDEX waze_segments_fwdDirection_btree ON "waze_segments" USING BTREE ((data->>'fwdDirection'));`)
          await postgres.query(`CREATE INDEX waze_segments_revDirection_btree ON "waze_segments" USING BTREE ((data->>'revDirection'));`)
          await postgres.query(`CREATE INDEX waze_segments_fromNodeID_btree ON "waze_segments" USING BTREE ((data->>'fromNodeID'));`)
          await postgres.query(`CREATE INDEX waze_segments_toNodeID_btree ON "waze_segments" USING BTREE ((data->>'toNodeID'));`)
          await postgres.query(`CREATE INDEX waze_segments_country_btree ON "waze_segments" USING BTREE ((data->>'_geoCountry'));`)
          await postgres.query(`CREATE INDEX waze_segments_state_btree ON "waze_segments" USING BTREE ((data->>'_geoState'));`)
          await postgres.query(`CREATE INDEX waze_segments_stateid_btree ON "waze_segments" USING BTREE ((data->>'_geoStateId'));`)
          await postgres.query(`CREATE INDEX waze_segments_streetname_btree ON "waze_segments" USING BTREE (("data"->>'_WZstreetName'));`)

        }

        if (collection === 'mapUpdateRequests') {

          await postgres.query(`DROP TABLE IF EXISTS waze_ur_type;`)
          await postgres.query(`CREATE TABLE "waze_ur_type" (id integer PRIMARY KEY, urTitle varchar(45) NOT NULL);`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('6','Incorrect turn');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('7','Incorrect address');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('8','Incorrect route');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('9','Missing roundabout');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('10','General error');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('11','Turn not allowed');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('12','Incorrect junction');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('13','Missing bridge overpass');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('14','Improper / Poor navigation instructions');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('15','Missing exit');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('16','Missing road');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('18','Missing Place');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('19','Closed road');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('21','Missing street name');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('22','Incorrect street prefix or suffix');`)
          await postgres.query(`INSERT INTO "waze_ur_type" VALUES ('23','Missing or invalid speed limit');`)

        }

        if (collection === 'problems') {

          await postgres.query(`DROP TABLE IF EXISTS waze_mp_type;`)
          await postgres.query(`CREATE TABLE "waze_mp_type" (id integer PRIMARY KEY, mpTitle varchar(60) NOT NULL);`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('1','Segment with abnormal geometry detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('2','Segment with no connections detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('3','Missing junction detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('5','Overlapping segments detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('6','Routing problem detected (segment with no exit)');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('7','Inconsistent road type detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('8','Short segment detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('10','Junction with more than 5 connected segments detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('11','Inconsistent segment direction detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('12','Unnecessary junctions detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('13','Improper ramp connection detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('14','Wrong road elevation detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('15','Very sharp turn detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('16','Irregular toll road detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('17','Segment without details detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('19','Irregular roundabout segment detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('20','Irregular roundabout segment detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('21','Wrong street name detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('22','Invalid dead end detected');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('23','Routing problem');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('50','Parking Lot set as a Point');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('51','Place not reachable');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('52','Place missing from Waze map');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('53','Unmatched Places');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('70','Missing Parking Lot Place');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('71','Missing Parking Lot Place');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('101','Driving direction mismatch');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('102','Missing junction');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('103','Missing road');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('104','Crossing roads with no junction node');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('105','Road type mismatch');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('106','Disallowed turn might be allowed');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('200','Suggested route frequently ignored');`)
          await postgres.query(`INSERT INTO "waze_mp_type" VALUES ('300','Road Closure Request');`)

        }


        // **************     


        const cursor = await db.collection(collection).find()
        
        const workers = []
        const count = await cursor.count()
        const THREADS = 10
        for (let index = 0; index < THREADS; index++) {
            const limit = count/THREADS
            const insert = `INSERT INTO waze_${collection} (name,geom, data) `+
                        "VALUES ($2,ST_SetSRID(ST_GeomFromGeoJSON($1), 4326), $3) "
                        + "ON CONFLICT (name) DO "+
                        "UPDATE SET geom = ST_SetSRID(ST_GeomFromGeoJSON($1), 4326)"
            const values = ['_id']
            const cursor = await db.collection(collection).find().sort( { _id: 1 } ).skip(limit*index).limit(limit +1)         
            workers.push(worker(cursor, insert, values))
        }
        const v = await Promise.all(workers)
      //   await postgres.query(`
      //     DROP TABLE IF EXISTS joined_waze_${collection};`)
      // await postgres.query(`
      //     CREATE TABLE joined_waze_${collection} (
      //       id serial PRIMARY KEY,
      //       name VARCHAR,
      //       geom geometry(Geometry,4326),
      //       substateid VARCHAR,
      //       stateid VARCHAR,
      //       countryid VARCHAR,
      //       substate VARCHAR,
      //       state VARCHAR,
      //       country VARCHAR
      //     );`)
      // await postgres.query(`
      //     INSERT INTO  joined_waze_${collection}(name, geom, substateid, stateid, countryid, substate, state, country)
      //     SELECT v.name, v.geom, s.substateid, s.stateid, s.countryid, s.substate, s.state, s.country
      //       FROM public.waze_${collection} v 
      //       JOIN public.substates s
      //       ON ST_Intersects(v.geom,s.geom);`)


        postgres.release()
        const d2 = new Date()
        console.log(v.reduce((past, curret)=>past+curret))
        console.log(v)
        console.log(errores)

        console.log(`${(d2-d1)/1000} ${collection} segs`)
    }

    async function copyPanelzTables() {
      const postgres_panelz = await pool_panelz.connect()
      const postgres = await pool.connect()
      await postgres.query(`DROP TABLE IF EXISTS substates;`)
      await postgres.query(`DROP INDEX IF EXISTS geom_substates`)
      await postgres.query(`DROP TABLE IF EXISTS tiles`)
      await postgres.query(`DROP INDEX IF EXISTS geom_tiles`)

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
      );`)
      await postgres.query(`
          CREATE INDEX IF NOT EXISTS geom_substates
          ON public.substates USING gist(geom);`)

      await postgres.query(`CREATE TABLE IF NOT EXISTS tiles (
          id serial PRIMARY KEY,
          name VARCHAR UNIQUE,
          west FLOAT,
          south FLOAT,
          east FLOAT,
          north FLOAT,
          zoom integer,
          geom geometry(Geometry,4326)
      );`)
      await postgres.query(`
          CREATE INDEX IF NOT EXISTS geom_tiles
          ON public.tiles USING gist(geom);`)
      let query, result, insert, exec

      query = `SELECT id, name, substateid, stateid, countryid, substate, env, state, country, geom
                      FROM substates`
      result = await postgres_panelz.query(query)
      insert = `INSERT INTO substates ( id, name, substateid, stateid, countryid, substate, env, state, country, geom) `+
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
      for (let ir = 0; ir < result.rows.length; ir++) {
        const row = result.rows[ir];
        exec = {
          text: insert,
          values: [ row.id, row.name, row.substateid, row.stateid, row.countryid, row.substate, row.env, row.state, row.country, row.geom],
        }
        await postgres.query(exec)
      }      
      
      query = `SELECT id, name, west, south, east, north, zoom, geom
                FROM tiles`
      result = await postgres_panelz.query(query)
      insert = `INSERT INTO tiles ( id, name, west, south, east, north, zoom, geom) `+
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
      for (let ir = 0; ir < result.rows.length; ir++) {
        const row = result.rows[ir];
        exec = {
          text: insert,
          values: [ row.id, row.name, row.west, row.south, row.east, row.north, row.zoom, row.geom],
        }
        await postgres.query(exec)
      }                        
      
      postgres.release()
      postgres_panelz.release()
    }

    await copyPanelzTables()

    await worker_collection_insert('segments', true)

    await worker_collection_insert('venues', true)

    await worker_collection_insert('bigJunctions', true)

    await worker_collection_insert('calles', true)

    await worker_collection_insert('ciudades', true)

    await worker_collection_insert('countries', true)

    await worker_collection_insert('country', true)

    await worker_collection_insert('gpids', true)

    await worker_collection_insert('junctions', true)

    await worker_collection_insert('majorTrafficEvents', true)

    await worker_collection_insert('managedAreas', true)

    await worker_collection_insert('managerIDs', true)

    await worker_collection_insert('mapComments', true)

    await worker_collection_insert('mapUpdateRequests', true)

    await worker_collection_insert('problems', true)

    await worker_collection_insert('restrictedAreas', true)

    await worker_collection_insert('roadClosures', true)

    await worker_collection_insert('state', true)

    await worker_collection_insert('states', true)

    await worker_collection_insert('streets', true)

    await worker_collection_insert('substate', true)

    await worker_collection_insert('userAreas', true)

    await worker_collection_insert('users', true)

    await worker_collection_insert('connections', true)

    await worker_collection_insert('nodes', true)

    await worker_collection_insert('cities', true)
    
    process.exit(0);
    
  } catch (error) {
    console.error(error)
    process.exit(1);
  }
})

