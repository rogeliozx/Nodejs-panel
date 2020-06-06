var parser = require('cron-parser');

const sleep = require('./sleep');
const requests = async (db, io) => {
  while (true) {
    var count = await db
      .collection('requests')
      .find()
      .count();
    if (count === 0) {
      var crawls = await db
        .collection('crawls')
        .find({ activate: { $ne: false }, run: { $lt: new Date() } })
        .sort({ run: 1 })
        .limit(1)
        .toArray();
      for (let crawlIndex = 0; crawlIndex < crawls.length; crawlIndex++) {
        const crawl = crawls[crawlIndex];
        // var bulk = db.collection("requests").initializeUnorderedBulkOp();
        var substates = await db
          .collection('substate')
          .find({ _id: { $in: crawl.substates } })
          .sort({ SubState: 1 })
          .toArray();
        for (
          let substatesIndex = 0;
          substatesIndex < substates.length;
          substatesIndex++
        ) {
          const substate = substates[substatesIndex];
          const properties = {
            substate: substate.substateID,
            zoom: parseInt(crawl.zoom)
          };
          // console.log(properties)
          var tiles = await db
            .collection('tiles')
            .find(properties)
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
                substate: substate._id,
                mapcomments: crawl.mapcomments,
                urs: crawl.urs,
                venues: crawl.venues,
                placeupdates: crawl.placeupdates,
                segments: crawl.segments,
                managedareas: crawl.managedareas,
                roadclosures: crawl.roadclosures,
                zoom: crawl.zoom
              };
              await io.db.collection('requests').updateOne(
                { Env, tile: tile._id },
                {
                  $set: request
                },
                { upsert: true }
              );
              // bulk.insert(request);
              // bulk.find( { Env, tile : tile._id, } ).upsert().update(
              //     {
              //       $set: request
              //     }
              //  );
            }
          }
        }
        // bulk.execute();

        if (crawl.crawlerHours === 0 || crawl.crawlerHours.trim() === '0') {
          db.collection('crawls').updateOne(
            { _id: crawl._id },
            { $set: { activate: false } }
          );
        } else {
          if (
            typeof crawl.crawlerHours === 'string' ||
            crawl.crawlerHours instanceof String
          ) {
            if (crawl.crawlerHours.trim() !== '0') {
              var interval = parser.parseExpression(crawl.crawlerHours);
              db.collection('crawls').updateOne(
                { _id: crawl._id },
                { $set: { activate: true, run: interval.next().toDate() } }
              );
            }
          }
        }
        // console.log("se crearon requests")
      }
    }
    await sleep(10000);
    // console.log("requests-cycle", new Date())
  }
};
module.exports = requests;
