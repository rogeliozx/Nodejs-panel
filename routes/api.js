var express = require('express');
var turf = require('@turf/turf');

const THREADS_TILES = 50
var tj = require('@mapbox/togeojson'),
    DOMParser = require('xmldom').DOMParser;

var Venues = require("./apis/venues");
var Segments = require("./apis/segments");
var Gpids = require("./apis/gpids");
var Substates = require("./apis/substates");
var Mapupdaterequests = require("./apis/mapupdaterequests");
var Problems = require("./apis/problems");
var BigJunctions = require("./apis/bigJunctions");
var MajorTrafficEvents = require("./apis/majorTrafficEvents");
var RoadClosures = require("./apis/roadClosures");
var MapComments = require("./apis/mapComments");

var ManagedAreas = require("./apis/managedAreas");
var Users = require("./apis/users");


function API(context){    

    var router = express.Router();
    async function intersect(poly1, poly2) {
        try {
            const intersect = turf.intersect(poly1, poly2);
            if (intersect) {
                return poly2
            }else {
                return null
            }
        } catch (error) {
            console.log(error)
            return null
        }
        
    }
    function generateSquares(bbox) {

        const _minX = (Math.floor(bbox[0]/0.05) * 0.05)
        const _minY = (Math.floor(bbox[1]/0.05) * 0.05)
        const _maxX = (Math.floor(bbox[2]/0.05) * 0.05) + 0.05
        const _maxY = (Math.floor(bbox[3]/0.05) * 0.05) + 0.05
        var polygons = []
        for (let indexY = _minY; indexY <= _maxY ; indexY+=0.05) {
            for (let indexX = _minX; indexX <= _maxX ; indexX+=0.05) {

                const minX = indexX
                const minY = indexY
                const maxX = indexX + 0.05
                const maxY = indexY + 0.05

                var polygon = turf.polygon([[[minX, minY], [maxX, minY], [maxX, maxY], [minX, maxY], [minX,minY]]]);
                polygons.push(polygon)
            }
        }
        var collection = turf.featureCollection(polygons);
        return collection
    }

    /* GET home page. */
    router.post('/kml', async function(req, res, next) {
        let file = req.files.file;
        var fileExt = file.name.split('.').pop()

        var converted = null
        if(fileExt.toLowerCase() == "kml"){
            console.log("Leyendo ...")
            var kml = new DOMParser().parseFromString(file.data.toString())
            console.log("Convirtiendo ...")
            converted = tj.kml(kml)
        }else if(fileExt.toLowerCase() == "geojson"){
            converted = JSON.parse(file.data.toString())
        }else {
            var tiles = {
                features : []
            }
            res.json({
                test:file.data.toString(),
                geo:tiles,
                totalTiles:0
            });
            return
        }
        console.log("Convertido")
        var newFeatures = []

        // converted.features.forEach(feature => {
        //     feature.geometry.geometries.forEach(geometry => {

        //         var cellSide = 5;
        //         var options = {units: 'kilometers'};
                
        //         var bbox = turf.bbox(converted);

        //         var squareGrid = generateSquares(bbox);
        //         squareGrid.features.forEach(square => {
        //             var intersection = turf.intersect(geometry, square);
        //             if (intersection) {
        //                 newFeatures.push(square)
        //             }
        //         });
        //         newFeatures.push(geometry)
        //     });
        // });

        var cellSide = 5;
        var options = {units: 'kilometers'};
        var totalTiles = 0
        var polyUnion = null
        async function parseTiles(geoObj, triangleled = 0) {
            // console.log(`Type ${geoObj.type}`)
            switch (geoObj.type) {
                case "FeatureCollection":
                    for (let featureI = 0; featureI < geoObj.features.length; featureI++) {
                        const feature = geoObj.features[featureI];
                        await parseTiles(feature, triangleled)
                    }
                    // geoObj.features.forEach(feature => {
                        
                    // })
                    break;
                case "Feature":
                    await parseTiles(geoObj.geometry, triangleled)
                    
                    break;
                case "GeometryCollection":
                    for (let geometryI = 0; geometryI < geoObj.geometries.length; geometryI++) {
                        const geometry = geoObj.geometries[geometryI];
                        await parseTiles(geometry, triangleled)
                    }
                    // geoObj.geometries.forEach(geometry => {
                    //     await parseTiles(geometry, triangleled)
                    // })
                    break;

                case "MultiPolygon":
                    for (let multiI = 0; multiI < geoObj.coordinates.length; multiI++) {
                        const multy = geoObj.coordinates[multiI];
                        var polygon = turf.polygon(multy);
                        await parseTiles(polygon, triangleled)
                    }
                    break;
                case "Polygon":
                    
                    var bbox = turf.bbox(geoObj);

                    var squareGrid = generateSquares(bbox);
                    var len = squareGrid.features.length
                    for (let squareI = 0; squareI < len; squareI+=THREADS_TILES) {
                        // console.log(`leyendo tile ${squareI} de ${len}`)
                        // const square = squareGrid.features[squareI];
                        // var intersection = turf.intersect(geoObj, square);
                        // if (intersection) {
                        //     newFeatures.push(square)
                        //     totalTiles += 1
                        // }
                        let threads = []
                        for (let t = squareI; (t < squareI + THREADS_TILES && t < len ); t++) {
                            const prom = intersect(geoObj,squareGrid.features[t]);
                            threads.push(prom)
                        }
                        let values = await Promise.all(threads)
                        for (let i = 0; i < values.length; i++) {
                            const obj = values[i];
                            if(obj){
                                // newFeatures.push(obj)
                                
                                totalTiles += 1
                            }
                        }
                    }                
                    newFeatures.push(geoObj)
                    
                    break;
                case "Point":
                    var bbox = turf.bbox(geoObj);

                    var squareGrid = generateSquares(bbox);
                    var len = squareGrid.features.length
                    for (let squareI = 0; squareI < len; squareI++) {
                        // console.log(`leyendo tile ${squareI} de ${len}`)
                        const square = squareGrid.features[squareI];
                        // newFeatures.push(square)
                        totalTiles += 1
                    }
                    newFeatures.push(geoObj)
                    
                    break;
            
                default:
                    break;
            }
        }
        await parseTiles(converted, 0)
        // newFeatures.push(polyUnion)
        // FeatureCollection -> Feature -> (Polygon | Point)
        // FeatureCollection = {
        //     type:String,
        //     features:Array[Feature]
        // }
        // Feature = {
        //     type:String,
        //     properties:Object,
        //     geometry:(Polygon | Point)
        // }
        // GeometryCollection = {
        //     type:String,
        //     geometries:Array(Polygon | Point)
        // }
        // Point = {
        //     type:String,
        //     coordinates:Array(lng,lat),
        // }
        // Polygon = {
        //     type:String,
        //     coordinates:Array( Array(lng,lat),Array(lng,lat), ...)
        // }

        tiles = {
            features : newFeatures
        }
        res.json({
            test:file.data.toString(),
            geo:tiles,
            totalTiles
        });
        
    });
    router.get('/', function(req, res, next) {
        res.json({
            test:"Hello World 222"
        });
    });
    router.post('/gpids', Gpids.list(context));
    router.post('/gpidsreact', Gpids.listReact(context));
    router.get('/cleangpids', Gpids.clean(context));

    router.post('/segments', Segments.list(context));
    router.get('/segments/querys', Segments.querys(context));
    router.get('/segments/filters', Segments.filters(context));

    router.post('/substates', Substates.list(context));
    router.get('/substates/querys', Substates.querys(context));
    router.get('/substates/filters', Substates.filters(context));
    
    router.post('/venues', Venues.list(context));
    router.get('/venues/querys', Venues.querys(context));
    router.get('/venues/filters', Venues.filters(context));

    router.post('/mapupdaterequests', Mapupdaterequests.list(context));
    router.get('/mapupdaterequests/querys', Mapupdaterequests.querys(context));
    router.get('/mapupdaterequests/filters', Mapupdaterequests.filters(context));

    router.post('/problems', Problems.list(context));
    router.get('/problems/querys', Problems.querys(context));
    router.get('/problems/filters', Problems.filters(context));

    router.post('/bigjunctions', BigJunctions.list(context));
    router.get('/bigjunctions/querys', BigJunctions.querys(context));
    router.get('/bigjunctions/filters', BigJunctions.filters(context));

    router.post('/majortrafficevents', MajorTrafficEvents.list(context));
    router.get('/majortrafficevents/querys', MajorTrafficEvents.querys(context));
    router.get('/majortrafficevents/filters', MajorTrafficEvents.filters(context));

    router.post('/roadclosures', RoadClosures.list(context));
    router.get('/roadclosures/querys', RoadClosures.querys(context));
    router.get('/roadclosures/filters', RoadClosures.filters(context));

    router.post('/mapcomments', MapComments.list(context));
    router.get('/mapcomments/querys', MapComments.querys(context));
    router.get('/mapcomments/filters', MapComments.filters(context));
    
    
    router.post('/managedareas', ManagedAreas.list(context));

    router.post('/users', Users.list(context));
    router.get('/users/querys', Users.querys(context));
    router.get('/users/filters', Users.filters(context));
    router.get('/users/clean', Users.clean(context));

    router.get('/getcountriescrawl', async function (req, res, next) {
        const { db } = context
        const subtates = db.collection('tiles').distinct('substate',{updated_at:{$exists:true}})
        const countri = db.collection('substate').distinct('countryID',{_id:{$in:['1700001.001','1700001.085']}})
        var countries = await db.collection("country").find().sort({Country:1}).toArray()
        res.json(countries);
    });
    router.get('/getstatescrawl/:Country', async function (req, res, next) {
        const { db } = context
        const {Country} = req.params
        const subtates = db.getCollection('tiles').distinct('substate',{updated_at:{$exists:true}})

        var states = await db.collection("state").find({Country}).sort({State:1}).toArray()
        res.json(states)
    });
    router.get('/getsubstatescrawl/:Country/:State', async function (req, res, next) {
        const { db } = context
        const {Country, State} = req.params
        var states = await db.collection("substate").find({Country, State}).sort({SubState:1}).toArray()
        res.json(states)
    });

    
    router.get('/getusers', async function (req, res, next) {
        const { db } = context
        const ids = await db.collection('managedAreas').distinct('userID')
        var users = await db.collection("users").find({_id:{$in:ids}}).sort({userName:1}).toArray()
        res.json(users);
    });
    router.get('/getcountries', async function (req, res, next) {
        const { db } = context
        const subtates = await db.collection('tiles').distinct('substate',{updated_at:{$exists:true}})
        const ids = await db.collection('substate').distinct('Country',{_id:{$in:subtates}})
        var countries = await db.collection("country").find({Country:{'$in':ids}}).sort({Country:1}).toArray()
        res.json(countries);
    });
    router.get('/getstates/:Country', async function (req, res, next) {
        const { db } = context
        const {Country} = req.params
        const subtates = await db.collection('tiles').distinct('substate',{updated_at:{$exists:true}})
        const ids = await db.collection('substate').distinct('State',{_id:{$in:subtates}})

        var states = await db.collection("state").find({Country, State:{'$in':ids}}).sort({State:1}).toArray()
        res.json(states)
    });
    router.get('/getsubstates/:Country/:State', async function (req, res, next) {
        const { db } = context
        const {Country, State} = req.params
        const subtates = await db.collection('tiles').distinct('substate',{updated_at:{$exists:true}})
        var states = await db.collection("substate").find({Country, State, _id:{$in:subtates}}).sort({SubState:1}).toArray()
        res.json(states)
    });
    router.get('/getcitiesvenues/:Country/:State/:SubState', async function (req, res, next) {
        const { db } = context
        const {Country, State, SubState} = req.params
        var cities = await db.collection("venues").distinct("_WZcityName",{
            _geoCountry:Country,
            _geoState:State,
            _geoSubState:SubState
        })
        res.json(cities)
    });
    router.get('/getcitiessegments/:Country/:State/:SubState', async function (req, res, next) {
        const { db } = context
        const {Country, State, SubState} = req.params
        var cities = await db.collection("segments").distinct("_WZcityName",{
            _geoCountry:Country,
            _geoState:State,
            _geoSubState:SubState
        })
        res.json(cities)
    });

    router.post('/recrawl', async function(req, res, next) {
        const { tileId, centroid=null } = req.body
        const { db } = context
        const valZoom = 0.05
        let _tileId = [tileId]
        if (centroid!==null) {
            let {coordinates:[lat, lng]} = centroid
            const west = parseFloat(((Math.floor(lat/valZoom) * valZoom)).toFixed(2))
            const south = parseFloat(((Math.floor(lng/valZoom) * valZoom)).toFixed(2))
            const east = parseFloat(((Math.floor(lat/valZoom) * valZoom) + valZoom).toFixed(2))
            const north = parseFloat(((Math.floor(lng/valZoom) * valZoom) + valZoom).toFixed(2))
            _tileId = [`${west},${south},${east},${north}`]
        }else{
            const [_west,_south,_east,_north] = tileId.split(',')
            const   west = parseFloat(_west),
                    south = parseFloat(_south),
                    east = parseFloat(_east),
                    north = parseFloat(_north)
            if ( ((north-south) > valZoom) ||  ((east-west) > valZoom)) {
                let fourTiles = []
                let nWest = west
                let nSouth = south
                fourTiles.push(`${nWest},${nSouth},${parseFloat((nWest+valZoom).toFixed(2))},${parseFloat((nSouth+valZoom).toFixed(2))}`)
                nWest = west
                nSouth = parseFloat((south + valZoom).toFixed(2))
                fourTiles.push(`${nWest},${nSouth},${parseFloat((nWest+valZoom).toFixed(2))},${parseFloat((nSouth+valZoom).toFixed(2))}`)
                nWest = parseFloat((west + valZoom).toFixed(2))
                nSouth = south
                fourTiles.push(`${nWest},${nSouth},${parseFloat((nWest+valZoom).toFixed(2))},${parseFloat((nSouth+valZoom).toFixed(2))}`)
                nWest = parseFloat((west + valZoom).toFixed(2))
                nSouth = parseFloat((south + valZoom).toFixed(2))
                fourTiles.push(`${nWest},${nSouth},${parseFloat((nWest+valZoom).toFixed(2))},${parseFloat((nSouth+valZoom).toFixed(2))}`)
                _tileId = fourTiles
            }
        }
        var tiles = await db.collection("tiles").find({_id:{'$in':_tileId}}).toArray()
        // console.log(tiles.length)
        for (let tilesIndex = 0; tilesIndex < tiles.length; tilesIndex++) {
            const tile = tiles[tilesIndex];
            const {Env:Envs=['row']} = tile
            for (let EnvIndex = 0; EnvIndex < Envs.length; EnvIndex++) {
                const Env = Envs[EnvIndex];
                const request = {
                    Env,
                    east : tile.east,
                    west : tile.west,
                    south : tile.south,
                    north : tile.north,
                    tile : tile._id,
                    substate : 'recrawl',
                    mapcomments: true,
                    urs: true,
                    venues: true,
                    placeupdates: false,
                    segments: true,
                    managedareas: true,
                    roadclosures: true,
                    recrawl:10
                }
                console.log(request)
                await db.collection("requests").insertOne(request);
            }
        }
        res.json({
            msj : 'requests created',
        });
    });






    function generateCorrds(latitude,longitude) {

        var delta_x = 0.05;
        var delta_y = 0.05;
        const lat = parseFloat(((Math.floor(latitude/delta_x) * delta_x)).toFixed(2))
        const lon = parseFloat(((Math.floor(longitude/delta_y) * delta_y)).toFixed(2))


        var initial_x = lat - (delta_x*2);
        var initial_y = lon - (delta_y*2);

        const _minX = lat - (delta_x*2);
        const _minY = lon - (delta_y*2);
        const _maxX = lat + (delta_x*2);
        const _maxY = lon + (delta_y*2);
        
        var squares = []
        for (let indexY = _minY; indexY <= _maxY ; indexY+=0.05) {
            for (let indexX = _minX; indexX <= _maxX ; indexX+=0.05) {

                const minX = parseFloat((indexX).toFixed(2))
                const minY = parseFloat((indexY).toFixed(2))
                const maxX = parseFloat((indexX + 0.05).toFixed(2))
                const maxY = parseFloat((indexY + 0.05).toFixed(2))

                squares.push({
                    minX,
                    minY,
                    maxX,
                    maxY
                })
            }
        }
        return { squares}
    }
    
    router.post('/requesttilelocation', async function(req, res, next) {
    // async function requestLatLon(req, res, db) {
        const { db } = context
        const {
            country='_ALL_',
            state='_ALL_',
            substate='_ALL_',
        } = req.body

        let match = {}
        if (country != "_ALL_") {
            match.Country = country
            if (state != "_ALL_") {
                match.State = state
                if (substate != "_ALL_") {
                    match.SubState = substate
                }
            }
        }
        coordsObjs=[]

        let center = {}
        const substates = await db.collection("substate").find(match).toArray()
        for (let substatesIndex = 0; substatesIndex < substates.length; substatesIndex++) {
            const substate = substates[substatesIndex];
            const properties = {
                substate:substate.substateID,
                zoom:4
            }
            var tiles = await db.collection("tiles").find(properties).toArray()
            for (let tilesIndex = 0; tilesIndex < tiles.length; tilesIndex++) {
                const tile = tiles[tilesIndex];
                const Env = substate.Env;
                const objSquare = {
                    maxY:tile.north,
                    minY:tile.south,
                    maxX:tile.east,
                    minX:tile.west
                }
                const {lng=tile.west, lat= tile.south} = center
                center = {lng:(lng+tile.west)/2, lat: (lat+tile.south)/2}
                coordsObjs.push({
                    coords:[
                            {lng:objSquare.minX,lat:objSquare.minY},
                            {lng:objSquare.maxX,lat:objSquare.minY},
                            {lng:objSquare.maxX,lat:objSquare.maxY},
                            {lng:objSquare.minX,lat:objSquare.maxY},
                        ],
                    id:tile._id
                })

                const request = {
                    Env,
                    east : tile.east,
                    west : tile.west,
                    south : tile.south,
                    north : tile.north,
                    tile : tile._id,
                    substate : 'recrawl',
                    mapcomments: true,
                    urs: true,
                    venues: true,
                    placeupdates: false,
                    segments: true,
                    managedareas: true,
                    roadclosures: true,
                    recrawl:10
                }
                await db.collection("requests")
                    .updateOne(
                        { Env, tile : tile._id, } ,
                        {
                            $set: request
                        },
                        { upsert: true }
                    )
            }
        }
        res.json({success:true,center,coordsObjs});
    });
    router.post('/requestLatLon', async function(req, res, next) {
    // async function requestLatLon(req, res, db) {
        const { db } = context
        const lat = req.body.lat;
        const lon = req.body.lon;
        const Env = req.body.server?req.body.server:'row';

        if (lat && lon) {

            const {squares} = generateCorrds(lat,lon);
            const bulk = [], coordsObjs=[]
            const bulkReq = db.collection("requests").initializeUnorderedBulkOp();
            for (let indexSquares = 0; indexSquares < squares.length; indexSquares++) {
                const objSquare = squares[indexSquares];
                
                const _minX = objSquare.minX
                const _minY = objSquare.minY
                const _maxX = objSquare.maxX
                const _maxY = objSquare.maxY


                const Tile = {
                    north: _maxY,
                    south: _minY,
                    east: _maxX,
                    west: _minX
                }
                const tileID = `${Tile.west},${Tile.south},${Tile.east},${Tile.north}`
                coordsObjs.push({
                    coords:[
                            {lng:objSquare.minX,lat:objSquare.minY},
                            {lng:objSquare.maxX,lat:objSquare.minY},
                            {lng:objSquare.maxX,lat:objSquare.maxY},
                            {lng:objSquare.minX,lat:objSquare.maxY},
                        ],
                    id:tileID
                })
                bulk.push({ updateOne :
                    {
                        "filter" : {  _id : tileID },
                        "update" : { 
                            $set : {
                              west:Tile.west,
                              south:Tile.south,
                              east:Tile.east,
                              north:Tile.north,
                              },
                            $addToSet: {Env}
                          },
                        "upsert" : true
                    }
                  })

                const request = {
                    Env,
                    east : Tile.east,
                    west : Tile.west,
                    south : Tile.south,
                    north : Tile.north,
                    tile : tileID,
                    substate : 'recrawl',
                    mapcomments: true,
                    urs: true,
                    venues: true,
                    placeupdates: false,
                    segments: true,
                    managedareas: true,
                    roadclosures: true,
                    recrawl:10
                }
                bulkReq.insert( request);
            }

            try{ 

                if (bulk.length > 0) {
                    const res = await db.collection('tiles').bulkWrite(bulk); 
                    bulkReq.execute();
                } 
                res.json({success:true,center:{lat:lon, lng: lat},coordsObjs});						
            }catch(err){
                console.log(err);
                res.json({success:false,err});
            }
        }else{
            res.json({success:false,error:"no hay tile"});
        }
    });
    return router
}
module.exports = API;