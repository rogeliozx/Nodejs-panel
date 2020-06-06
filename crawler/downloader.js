const sleep = require('./sleep')
const https = require('https')
var request = require('request');
const WAZE_MUR_URL = 'https://beta.waze.com/row-Descartes-live/app/MapProblems/UpdateRequests?ids='
var j = request.jar()
// request = request.defaults({jar: j});
const cooks = [
	'_web_session',
	'_csrf_token',
	'AWSELB'
]
let cookiesJson = []
try {
    cookiesJson = require('../cookies.json')
    cookiesJson = cookiesJson.filter(c=>cooks.indexOf(c.name)>=0)
    cookiesJson.forEach(c=>{
        var cookie = request.cookie(`${c.name}=${c.value}`);
        j.setCookie(cookie, 'https://'+c.domain);
        console.log(j.getCookies('https://'+c.domain))
    })
} catch (error) {
    console.log("Error en leer el archivo de cookies.json")
}

const THREADS = 10
let downloaded = 0 
const downloader = async (db, io) => {

    const download = async (array) => {
        // let solved = []
        while (array.length > 0) {
            const request = array.pop()
            const _mapcomments = request.mapcomments === true ? true : false
            const _urs = request.urs === true ? true : false
            const _venues = request.venues === true ? true : false
            const _placeupdates = request.placeupdates === true ? true : false
            const _segments = request.segments === true ? true : false
            const _managedareas = request.managedareas === true ? true : false
            const _roadclosures = request.roadclosures === true ? true : false
            const _zoom = request.zoom
            // console.log(request)
            
            let server = 'Descartes'
            switch (request.Env) {
                case 'na':
                    server = 'Descartes'
                    break;
                case 'row':
                    server = 'row-Descartes'
                    break;
                case 'il':
                    server = 'il-Descartes'
                    break;
                default:
                    server = 'Descartes'
                    break;
            }
            let url = `https://beta.waze.com/${server}/app/Features?bbox=${request.west},${request.south},${request.east},${request.north}`
            url += (cookiesJson.length===0)?`&sandbox=true` : ''
            url += (_roadclosures)?`&roadClosures=true` : ''
            url += (_roadclosures)?`&majorTrafficEvents=true` : ''
            url += (_managedareas)?`&managedAreas=true` : ''
            url += (_mapcomments)?`&mapComments=true` : ''
            url += (_urs)?`&mapUpdateRequestFilter=1,0` : ''
            url += (_urs)?`&problemFilter=0` : ''
            url += ( _placeupdates)?`&venueLevel=${_zoom}` : ''
            
            url += (_venues )?`&venueLevel=4` : ''
            
            url += (_venues)?`&venueFilter=3,3,3` : ''
            url += (_placeupdates)?`&venueFilter=3,3,3` : ''
            url += (_segments)?`&roadTypes=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22` : ''
            // console.log(url)
            try {
                const json = await get(url)
                if (json) {
                    json.url = url
                    json.substate = request.substate
                    json.tile = request.tile
                    json.Env = request.Env
                    json.placeupdates = request._placeupdates
                    json.zoom = _zoom
                    if (request.recrawl > 0) {
                        json.recrawl = request.recrawl
                    }
                    
                    const { mapUpdateRequests:{objects:mapUpdateRequests=[]}={}} = json
                    let urIds = []
                    let newMapUpdateRequests = {}
                    for (let indexUR = 0; indexUR < mapUpdateRequests.length; indexUR++) {
                        const UR = mapUpdateRequests[indexUR];
                        newMapUpdateRequests[UR.id] = UR
                        urIds.push(UR.id)
                        if (urIds.length>=10) {
                            const urUrl = WAZE_MUR_URL + urIds.join(',')
                            const urJson = await get(urUrl)
                            const {updateRequestSessions:{objects:updateRequestSessions=[]}={}} = urJson
                            updateRequestSessions.forEach(urs=>{
                                newMapUpdateRequests[urs.id].comments = urs.comments
                                if (urs.comments.length === 0) {
                                    newMapUpdateRequests[urs.id]._section = 0
                                }else{
                                    const {userID} = urs.comments[urs.comments.length -1]
                                    if (userID === -1) {
                                        newMapUpdateRequests[urs.id]._section = 1
                                    }else{
                                        newMapUpdateRequests[urs.id]._section = 2
                                    }
                                }
                            })
                            urIds=[]
                        }
                    }
                    if (urIds.length>0) {
                        const urUrl = WAZE_MUR_URL + urIds.join(',')
                        const urJson = await get(urUrl)
                        const {updateRequestSessions:{objects:updateRequestSessions=[]}={}} = urJson
                        updateRequestSessions.forEach(urs=>{
                            newMapUpdateRequests[urs.id].comments = urs.comments
                            if (urs.comments.length === 0) {
                                newMapUpdateRequests[urs.id]._section = 0
                            }else{
                                const userId = urs.comments[urs.comments.length -1]
                                if (userId === -1) {
                                    newMapUpdateRequests[urs.id]._section = 1
                                }else{
                                    newMapUpdateRequests[urs.id]._section = 2
                                }
                            }
                        })
                        urIds=[]
                    }
                    json.mapUpdateRequests = {
                        objects:Object.values(newMapUpdateRequests)
                    }
                    await db.collection("jsons").createIndex( { size: 1 });
                    await db.collection("jsons").insertOne(json)
                    await db.collection("requests").deleteOne( { _id: request._id } )
                    downloaded = downloaded+1
                    io.sockets.emit('stats', {
                        downloaded
                    });
                    // solved.push(request._id)
                }
            } catch (error) {
                console.error(url, error)
                request.errors = request.errors? request.errors +1 : 1
                if (request.errors < 5) {
                    array.push(request)
                }
            }            
        }
    }
    await db.collection('requests').createIndex( { recrawl:1 });
    while (true) {
        io.sockets.emit('stats', {
            downloaded
        });
        let requests = await db.collection("requests").find().sort({recrawl:1}).limit(1000).toArray()
        if (requests.length > 0) {
            let requestsIDS = requests.map(r=>r._id)
            await db.collection("requests").updateMany(
                {
                    _id:{ $in: requestsIDS }
                },
                {
                    $set: {
                       working:true
                    }
                }
            )
            let threads = []
            for (let t = 0; t < THREADS; t++) {
                const thread = download(requests);
                threads.push(thread)
            }
            let values = await Promise.all(threads)
        }
        await sleep(1000);
        // console.log("requests-downloader", new Date())
    }

}
module.exports = downloader;




function get(url) {
	return new Promise((resolve, reject) => {
		request({url,jar: j,json:true}, function (error, response, body) {
			if (error) {
				reject(error)
			}else{
				resolve(body);
			}
		});
	});
}

function pget(url) {
    return new Promise((resolve, reject) => {
        https.get(url, (res) => {
            const statusCode = res.statusCode;
            const contentType = res.headers['content-type'];
        
            let error;
            if (statusCode !== 200) {
                error = new Error('Request Failed.\n' +
                                `Status Code: ${statusCode}`);
            } else if (!/^application\/json/.test(contentType)) {
                error = new Error('Invalid content-type.\n' +
                                `Expected application/json but received ${contentType}`);
            }
            if (error) {
                // console.log(error.message);
                reject(error.message)
                // consume response data to free up memory
                res.resume(error);
                return;
            }
        
            res.setEncoding('utf8');
            let rawData = '';
            res.on('data', (chunk) => rawData += chunk);
            res.on('end', () => {
            try {
                const parsedData = JSON.parse(rawData);
                parsedData.size = rawData.length
                resolve(parsedData);
            } catch (e) {
                // console.log(e.message);
                reject(e)
            }
            });
        }).on('error', (e) => {
            // console.log(`Got error: ${e.message}`);
            reject(e)
        });
    });
}
