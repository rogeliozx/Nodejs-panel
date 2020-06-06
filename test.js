const { errores, worker, worker_substate, worker_substates_create, worker_collection_insert, worker_collection_geo } = require('./crawler/intersectsTools')
var MongoClient = require('mongodb').MongoClient	;

var database = "panelz";
var url = "mongodb://localhost:27017/"+database+"?maxPoolSize=434";

MongoClient.connect(url,{ useNewUrlParser: true, useUnifiedTopology: true }, async function(err, dbase) {
  if (err) throw err;
  try {
    var db = dbase.db(database);

    // Este metodo crea los substates dentro de POSTGIS
    // params:
    //    drop:- para saber si va a borrar la tabla(true) o no(false) de substates en postgres
    // await worker_substates_create(true)

    // Este metodo inserta los documents(venues/segments/etc.) en POSTGRES y crea los intersects
    // params:
    //    collection:- nombre de la collecion que va a leer de mongo
    //    drop:- para saber si va a borrar la tabla(true) o no(false) de la collection dada en postgres
    // await worker_collection_insert('venues', true)

    // Este metodo lee la tabla de interesects de POSTGRES y los empareja con los documents de mongo
    // para agregar substate/state/country al document
    // params:
    //    collection:-  nombre de la collecion de mongo donde va a actualizar los intersects
    //    idNumber:-    algunas collections como segments sus IDs son integer,
    //                  POSTGRES necesita saber esto para poder emparejar los objetos,
    //                  por default es false
    //                  ej:- await  worker_collection_geo('segments', true)
    //                  ej:- await  worker_collection_geo('venues')

    const d1 = new Date()
    let processData = false
    processData = await worker_collection_insert(db, 'venues', true, true)
    if(processData)await worker_collection_geo(db, 'venues')

    await worker_collection_insert(db, 'segments', true)
    await worker_collection_geo(db, 'segments', true)

    await worker_collection_insert(db, 'cities', true)
    await worker_collection_geo(db, 'cities', true)

    await worker_collection_insert(db, 'mapComments', true)
    await worker_collection_geo(db, 'mapComments')

    await worker_collection_insert(db, 'roadClosures', true)
    await worker_collection_geo(db, 'roadClosures')

    await worker_collection_insert(db, 'bigJunctions', true)
    await worker_collection_geo(db, 'bigJunctions', true)

    await worker_collection_insert(db, 'junctions', true)
    await worker_collection_geo(db, 'junctions', true)

    await worker_collection_insert(db, 'managedAreas', true)
    await worker_collection_geo(db, 'managedAreas', true)

    await worker_collection_insert(db, 'mapUpdateRequests', true)
    await worker_collection_geo(db, 'mapUpdateRequests', true)

    await worker_collection_insert(db, 'restrictedAreas', true)
    await worker_collection_geo(db, 'restrictedAreas', true)

    await worker_collection_insert(db, 'problems', true)
    await worker_collection_geo(db, 'problems')

    await worker_collection_insert(db, 'gpids', true)
    await worker_collection_geo(db, 'gpids')

    await worker_collection_insert(db, 'nodes', true)
    await worker_collection_geo(db, 'nodes', true)

    const d2 = new Date()

    console.log(`TOTAL TIME:- ${(d2-d1)/1000} segs`)
    process.exit(0);

  } catch (error) {
    console.error(error)
    process.exit(1);
  }
})

