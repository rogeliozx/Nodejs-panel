var ObjectID = require('mongodb').ObjectID;
const requests = require('./requests')
const downloader = require('./downloader')
const processor = require('./processor')
const intersects = require('./intersects')

function crawler(db, io) {
    const run = async () =>{
        requests(db,io)
        downloader(db,io)
        processor(db,io)
        intersects(db,io)
    }
    return {
        run
    }
} 
module.exports = crawler;