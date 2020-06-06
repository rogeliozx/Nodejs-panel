const sleep = require('./sleep');
var turf = require('@turf/turf');
const THREADS = 10;
let processed = 0;
let processedIds = [];
const processor = async (db, io) => {
  function intersect(poly1, poly2) {
    try {
      const intersect = turf.intersect(poly1, poly2);
      if (intersect) {
        return { intersect, tile: poly2 };
      } else {
        return null;
      }
    } catch (error) {
      // console.log(error)
      return null;
    }
  }

  const loadBulk = async (col, bulk) => {
    let reload = true;
    while (reload) {
      try {
        await db.collection(col).bulkWrite(bulk);
        reload = false;
      } catch (error) {
        // console.log(bulks[elem])
        console.log('error en ', col);
        console.log(error);
        await sleep(2000);
      }
    }
  };
  const proces = async array => {
    // let solved = []
    while (true) {
      if (array.length > 0) {
        await sleep(Math.floor(Math.random() * 101));
        const jsonid = array.pop();
        if (jsonid) {
          try {
            const json = await db
              .collection('jsons')
              .findOne({ _id: jsonid._id });
            if (typeof json === 'string' || json instanceof String) {
              console.log(json);
            } else if (json) {
              const Tile = await db
                .collection('tiles')
                .findOne({ _id: json.tile });
              // var elements = Object.keys(json).filter(e => (e==='_id')?false:true)
              var elements = [
                'countries',
                'states',
                'cities',
                'streets',
                'users',
                'connections'
              ];
              var _elements = Object.keys(json);
              for (var i = 0; i < _elements.length; i++) {
                if (
                  _elements[i] != 'countries' &&
                  _elements[i] != 'states' &&
                  _elements[i] != 'cities' &&
                  _elements[i] != 'streets' &&
                  _elements[i] != 'users' &&
                  _elements[i] != 'connections' &&
                  // ignore this variables
                  _elements[i] != '_id' &&
                  _elements[i] != 'size' &&
                  _elements[i] != 'url' &&
                  _elements[i] != 'substate' &&
                  _elements[i] != 'tile' &&
                  _elements[i] != 'working' &&
                  _elements[i] != 'Env' &&
                  _elements[i] != 'recrawl' &&
                  _elements[i] != 'placeupdates'
                ) {
                  elements.push(_elements[i]);
                }
              }

              var tamanios = {
                segments_ids: [],
                venues_ids: [],
                gpids: []
              };
              var bulks = {
                segments_ids: [],
                venues_ids: [],
                gpids: [],
                ciudades: [],
                calles: []
              };

              var countries = {};
              var states = {};
              var cities = {};
              var streets = {};
              var users = {};
              var conns = {};

              for (var i = 0; i < elements.length; i++) {
                var elem = elements[i];
                var segsJson = json[elem];
                if (segsJson != undefined) {
                  var segs = segsJson.objects;

                  if (elem === 'connections') {
                    segs = [];
                    var keys = Object.keys(segsJson);
                    for (var i3 = 0; i3 < keys.length; i3++) {
                      var key = keys[i3];
                      segs.push({
                        id: key,
                        data: segsJson[key],
                        segmentID: parseInt(key)
                      });
                      conns[key] = key;
                    }
                  }

                  if (segs != undefined) {
                    tamanios[elem + '_ids'] = [];
                    tamanios[elem + '_ids_intersect'] = [];

                    bulks[elem] = [];
                    for (var e = 0; e < segs.length; e++) {
                      var seg =
                        typeof segs[e] === 'object' ? segs[e] : { id: segs[e] };
                      seg.tile = json.tile;
                      seg.Env = json.Env;
                      if (json.recrawl > 0) {
                        seg.recrawl = 10;
                      }

                      if (elem === 'countries') {
                        countries[seg.id] = {
                          name: seg.name,
                          id: seg.id
                        };
                      }
                      if (elem === 'states') {
                        if (countries[seg.countryID]) {
                          states[seg.id] = {
                            countryId: countries[seg.countryID].id,
                            countryName: countries[seg.countryID].name,
                            name: seg.name,
                            id: seg.id
                          };
                        } else if (Object.keys(countries).length === 1) {
                          var country = countries[Object.keys(countries)[0]];
                          if (seg.countryID === 0) {
                            seg.countryID = country.id;
                          }
                          states[seg.id] = {
                            countryId: country.id,
                            countryName: country.name,
                            name: seg.name,
                            id: seg.id
                          };
                        } else {
                          states[seg.id] = {
                            countryId: -1,
                            countryName: '',
                            name: seg.name,
                            id: seg.id
                          };
                        }
                      }
                      if (elem === 'cities') {
                        if (states[seg.stateID]) {
                          cities[seg.id] = {
                            stateId: states[seg.stateID].id,
                            stateName: states[seg.stateID].name,
                            countryId: states[seg.stateID].countryId,
                            countryName: states[seg.stateID].countryName,
                            name: seg.name,
                            id: seg.id
                          };
                        } else if (Object.keys(countries).length === 1) {
                          var country = countries[Object.keys(countries)[0]];
                          cities[seg.id] = {
                            stateId: -1,
                            stateName: '',
                            countryId: country.id,
                            countryName: country.name,
                            name: seg.name,
                            id: seg.id
                          };
                        } else {
                          cities[seg.id] = {
                            stateId: -1,
                            stateName: '',
                            countryId: -1,
                            countryName: '',
                            name: seg.name,
                            id: seg.id
                          };
                        }

                        bulks['ciudades'].push({
                          updateOne: {
                            filter: { _id: seg.id },
                            update: { $set: { ...cities[seg.id] } },
                            upsert: true
                          }
                        });
                      }
                      if (elem === 'bigJunctions') {
                        if (cities[seg.cityID]) {
                          seg.cityName = cities[seg.cityID].name;
                        }
                      }
                      if (elem === 'streets') {
                        if (cities[seg.cityID]) {
                          streets[seg.id] = {
                            stateId: cities[seg.cityID].stateId,
                            stateName: cities[seg.cityID].stateName,
                            countryId: cities[seg.cityID].countryId,
                            countryName: cities[seg.cityID].countryName,
                            cityId: cities[seg.cityID].id,
                            cityName: cities[seg.cityID].name,
                            name: seg.name,
                            id: seg.id
                          };
                        } else if (Object.keys(states).length === 1) {
                          var state = states[Object.keys(states)[0]];
                          streets[seg.id] = {
                            stateId: state.id,
                            stateName: state.name,
                            countryId: state.countryId,
                            countryName: state.countryName,
                            cityId: -1,
                            cityName: '',
                            name: seg.name,
                            id: seg.id
                          };
                        } else if (Object.keys(countries).length === 1) {
                          var country = countries[Object.keys(countries)[0]];
                          streets[seg.id] = {
                            stateId: -1,
                            stateName: '',
                            countryId: country.id,
                            countryName: country.name,
                            cityId: -1,
                            cityName: '',
                            name: seg.name,
                            id: seg.id
                          };
                        } else {
                          streets[seg.id] = {
                            stateId: -1,
                            stateName: '',
                            countryId: -1,
                            countryName: '',
                            cityId: -1,
                            cityName: '',
                            name: seg.name,
                            id: seg.id
                          };
                        }

                        bulks['calles'].push({
                          updateOne: {
                            filter: { _id: seg.id },
                            update: { $set: { ...streets[seg.id] } },
                            upsert: true
                          }
                        });
                      }

                      if (elem === 'users') {
                        users[seg.id] = seg;
                      }
                      if (elem === 'managedAreas') {
                        if (users[seg.userID]) {
                          seg.userName = users[seg.userID].userName;
                        }
                      }
                      if (seg.createdBy) {
                        if (users[seg.createdBy]) {
                          seg._createdBy = users[seg.createdBy].userName;
                          seg._createdByRank = users[seg.createdBy].rank;
                        } else {
                          seg._createdBy = '';
                          seg._createdByRank = '';
                        }
                      }
                      if (seg.updatedBy) {
                        if (users[seg.updatedBy]) {
                          seg._updatedBy = users[seg.updatedBy].userName;
                          seg._updatedByRank = users[seg.updatedBy].rank;
                        } else {
                          seg._updatedBy = '';
                          seg._updatedByRank = '';
                        }
                      }
                      if (seg.cityID) {
                        var city = cities[seg.cityID];
                        if (city) {
                          seg._WZstateId = city.stateId;
                          seg._WZstateName = city.stateName;
                          seg._WZcountryId = city.countryId;
                          seg._WZcountryName = city.countryName;
                          seg._WZcityId = city.id;
                          seg._WZcityName = city.name;
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        }
                      }
                      if (elem === 'mapComments') {
                        let conversation = seg.conversation
                          ? seg.conversation
                          : [];
                        for (
                          let indexConv = 0;
                          indexConv < conversation.length;
                          indexConv++
                        ) {
                          let conv = conversation[indexConv];
                          if (conv.userID && users[conv.userID]) {
                            conv.userName = users[conv.userID].userName;
                          }
                        }
                        seg.conversation = conversation;
                      }
                      if (elem === 'segments') {
                        var streetIDs = Array.isArray(seg.streetIDs)
                          ? seg.streetIDs
                          : [];
                        var streetIDNames = [];
                        for (
                          var iStreetID = 0;
                          iStreetID < streetIDs.length;
                          iStreetID++
                        ) {
                          var streetID = streetIDs[iStreetID];
                          if (streets[streetID]) {
                            streetIDNames.push(streets[streetID].name);
                          }
                        }

                        seg._streetIDNames = streetIDNames;

                        if (streets[seg.primaryStreetID]) {
                          seg._WZstateId = streets[seg.primaryStreetID].stateId;
                          seg._WZstateName =
                            streets[seg.primaryStreetID].stateName;
                          seg._WZcountryId =
                            streets[seg.primaryStreetID].countryId;
                          seg._WZcountryName =
                            streets[seg.primaryStreetID].countryName;
                          seg._WZcityId = streets[seg.primaryStreetID].cityId;
                          seg._WZcityName =
                            streets[seg.primaryStreetID].cityName;
                          seg._WZstreetName = streets[seg.primaryStreetID].name;
                          seg._WZstreetId = streets[seg.primaryStreetID].id;
                        } else if (Object.keys(streets).length === 1) {
                          var street = streets[Object.keys(streets)[0]];
                          seg._WZstateId = street.stateId;
                          seg._WZstateName = street.stateName;
                          seg._WZcountryId = street.countryId;
                          seg._WZcountryName = street.countryName;
                          seg._WZcityId = street.cityId;
                          seg._WZcityName = street.cityName;
                          seg._WZstreetName = street.name;
                          seg._WZstreetId = street.id;
                        } else if (Object.keys(cities).length === 1) {
                          var city = cities[Object.keys(cities)[0]];
                          seg._WZstateId = city.stateId;
                          seg._WZstateName = city.stateName;
                          seg._WZcountryId = city.countryId;
                          seg._WZcountryName = city.countryName;
                          seg._WZcityId = city.id;
                          seg._WZcityName = city.name;
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else if (Object.keys(states).length === 1) {
                          var state = states[Object.keys(states)[0]];
                          seg._WZstateId = state.id;
                          seg._WZstateName = state.name;
                          seg._WZcountryId = state.countryId;
                          seg._WZcountryName = state.countryName;
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else if (Object.keys(countries).length === 1) {
                          var country = countries[Object.keys(countries)[0]];
                          seg._WZstateId = -1;
                          seg._WZstateName = '';
                          seg._WZcountryId = country.id;
                          seg._WZcountryName = country.name;
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else {
                          seg._WZstateId = -1;
                          seg._WZstateName = '';
                          seg._WZcountryId = -1;
                          seg._WZcountryName = '';
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        }

                        seg.error_fwdDirection = false;
                        seg.error_revDirection = false;
                        if (seg.fwdDirection) {
                          if (!conns[seg.id + 'f']) {
                            seg.error_fwdDirection = true;
                          }
                        }
                        if (seg.revDirection) {
                          if (!conns[seg.id + 'r']) {
                            seg.error_revDirection = true;
                          }
                        }
                      }
                      if (elem === 'venues') {
                        if (streets[seg.streetID]) {
                          seg._WZstateId = streets[seg.streetID].stateId;
                          seg._WZstateName = streets[seg.streetID].stateName;
                          seg._WZcountryId = streets[seg.streetID].countryId;
                          seg._WZcountryName =
                            streets[seg.streetID].countryName;
                          seg._WZcityId = streets[seg.streetID].cityId;
                          seg._WZcityName = streets[seg.streetID].cityName;
                          seg._WZstreetName = streets[seg.streetID].name;
                          seg._WZstreetId = streets[seg.streetID].id;
                        } else if (Object.keys(streets).length === 1) {
                          var street = streets[Object.keys(streets)[0]];
                          seg._WZstateId = street.stateId;
                          seg._WZstateName = street.stateName;
                          seg._WZcountryId = street.countryId;
                          seg._WZcountryName = street.countryName;
                          seg._WZcityId = street.cityId;
                          seg._WZcityName = street.cityName;
                          seg._WZstreetName = street.name;
                          seg._WZstreetId = street.id;
                        } else if (Object.keys(cities).length === 1) {
                          var city = cities[Object.keys(cities)[0]];
                          seg._WZstateId = city.stateId;
                          seg._WZstateName = city.stateName;
                          seg._WZcountryId = city.countryId;
                          seg._WZcountryName = city.countryName;
                          seg._WZcityId = city.id;
                          seg._WZcityName = city.name;
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else if (Object.keys(states).length === 1) {
                          var state = states[Object.keys(states)[0]];
                          seg._WZstateId = state.id;
                          seg._WZstateName = state.name;
                          seg._WZcountryId = state.countryId;
                          seg._WZcountryName = state.countryName;
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else if (Object.keys(countries).length === 1) {
                          var country = countries[Object.keys(countries)[0]];
                          seg._WZstateId = -1;
                          seg._WZstateName = '';
                          seg._WZcountryId = country.id;
                          seg._WZcountryName = country.name;
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        } else {
                          seg._WZstateId = -1;
                          seg._WZstateName = '';
                          seg._WZcountryId = -1;
                          seg._WZcountryName = '';
                          seg._WZcityId = -1;
                          seg._WZcityName = '';
                          seg._WZstreetName = '';
                          seg._WZstreetId = -1;
                        }
                        if (users[seg.createdBy]) {
                          seg._createdBy = users[seg.createdBy].userName;
                          seg._createdByRank = users[seg.createdBy].rank;
                        } else {
                          seg._createdBy = '';
                          seg._createdByRank = '';
                        }
                        if (users[seg.updatedBy]) {
                          seg._updatedBy = users[seg.updatedBy].userName;
                          seg._updatedByRank = users[seg.updatedBy].rank;
                        } else {
                          seg._updatedBy = '';
                          seg._updatedByRank = '';
                        }
                      }
                      if (seg.geometry) {
                        try {
                          switch (seg.geometry.type) {
                            case 'Polygon':
                              seg._area = turf.area(seg.geometry);
                              break;
                            case 'LineString':
                              let options = { units: 'meters' };
                              seg._length = turf.length(seg.geometry, options);
                              break;

                            default:
                              break;
                          }
                        } catch (error) {
                          console.error(error);
                        }
                        if (elem == 'segments') {
                          let line = turf.lineString(seg.geometry.coordinates); // Segments collection
                          let options = { units: 'meters' };
                          let _length = turf.length(line, { units: 'meters' });
                          let distance = _length / 2;
                          let along = turf.along(line, distance, options);
                          seg.centroid = along;
                        } else {
                          seg.centroid = turf.centroid(seg.geometry);
                        }
                        if (seg.centroid) {
                          const cX = seg.centroid.geometry.coordinates[0];
                          const cY = seg.centroid.geometry.coordinates[1];
                          if (
                            Tile.west <= cX &&
                            cX <= Tile.east &&
                            Tile.south <= cY &&
                            cY <= Tile.north
                          ) {
                            tamanios[elem + '_ids_intersect'].push(seg.id);
                          }
                          seg.centroid = seg.centroid.geometry;
                        }
                      }
                      tamanios[elem + '_ids'].push(seg.id);
                      bulks[elem].push({
                        updateOne: {
                          filter: { _id: seg.id },
                          update: { $set: seg },
                          upsert: true
                        }
                      });
                    }
                    tamanios[elem] = bulks[elem].length;
                  }
                }
              }

              elements.push('ciudades');
              elements.push('calles');
              elements.push('gpids');
              var newTile = { updated_at: new Date(), ...tamanios };

              if (
                newTile.segments_ids.length <= 0 &&
                newTile.venues_ids.length <= 0
              ) {
                newTile.is_empty = true;
              } else {
                newTile.is_empty = false;
              }

              for (var i = elements.length - 1; i >= 0; i--) {
                var elem = elements[i];

                await db.collection(elem).createIndex({ updatedBy: 1 });

                await db.collection(elem).createIndex({ tile: 1 });
                await db.collection(elem).createIndex({ recrawl: 1 });
                if (elem === 'gpids' || json.placeupdates === true) {
                  const msj = 'no borrar';
                } else {
                  await db.collection(elem).deleteMany({ tile: json.tile });
                }

                if (bulks[elem]) {
                  if (bulks[elem].length > 0) {
                    await loadBulk(elem, bulks[elem]);
                    bulks[elem] = [];
                  }
                }
              }
              if (io) {
                io.emit('updatedTiles', {
                  ...newTile,
                  is_empty: newTile.is_empty,
                  tile: json.tile,
                  id: json.tile,
                  _id: json.tile
                });
              }
              await db.collection('tiles').bulkWrite([
                {
                  updateOne: {
                    filter: { _id: json.tile },
                    update: { $set: { ...newTile } },
                    upsert: true
                  }
                }
              ]);

              // console.log(json)
              await db.collection('jsons').deleteOne({ _id: json._id });
              processed = processed + 1;
              processedIds.push(json._id);
              io.sockets.emit('stats', {
                processed,
                processedIds
              });
            }
          } catch (error) {
            console.error(jsonid);
            console.error(error);
            jsonid.errors = jsonid.errors ? jsonid.errors + 1 : 1;
            if (jsonid.errors < 5) {
              array.push(jsonid);
            }
          }
        }
      } else {
        await sleep(1000);
      }
      // console.log(`Faltan -- ${array.length}`)
    }
  };

  let threads = [];
  let JSONS = [];
  for (let t = 0; t < THREADS; t++) {
    const thread = proces(JSONS);
    threads.push(thread);
  }
  await db.collection('jsons').createIndex({ recrawl: 1 });
  while (true) {
    io.sockets.emit('stats', {
      processed
    });
    // console.log(jsons)
    if (JSONS.length < THREADS) {
      let jsonsLeft = await db
        .collection('jsons')
        .find()
        .sort({ size: 1 })
        .sort({ recrawl: 1 })
        .limit(100)
        .project({ _id: 1 })
        .toArray();
      if (jsonsLeft.length > 0) {
        let resultsIDS = jsonsLeft.map(r => r._id);
        await db.collection('jsons').updateMany(
          {
            _id: { $in: resultsIDS }
          },
          {
            $set: {
              working: true
            }
          }
        );
        for (
          let jsonsLeftIndex = 0;
          jsonsLeftIndex < jsonsLeft.length;
          jsonsLeftIndex++
        ) {
          const left = jsonsLeft[jsonsLeftIndex];
          JSONS.push(left);
        }
      }
      // console.log('----AGREGO 100')
    }

    await sleep(1000);
    // console.log("jsons-processor", new Date())
  }
};
module.exports = processor;
