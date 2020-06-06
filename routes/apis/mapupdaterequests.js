
var exports = module.exports = {};


const MAPUPDATEREQUESTS_FILTERS = [
]

const mapupdaterequestsQuerys = {
}

exports.filters = context => async function (req, res, next) {
    res.json(MAPUPDATEREQUESTS_FILTERS);
}

exports.list = context => async function(req, res, next) {
    const { db } = context
    const { 
        country="_ALL_",
        state="_ALL_",
        substate="_ALL_", 
        query=0, 
        filter={}
    } = req.body

    let match = {}
    for (let index = 0; index < MAPUPDATEREQUESTS_FILTERS.length; index++) {
        const f = MAPUPDATEREQUESTS_FILTERS[index];
        let fv = filter[f.name]
        if (f.number === true) {
            const n = Number(fv)
            if (!Number.isNaN(n)) {
                fv = n
            }
        }
        if (fv !== undefined) {
            switch (f.type) {
                case 'select':
                    match[f.name] = fv
                    break;
                case 'boolean':
                    match[f.name] = fv === true
                    break;
                case 'text':
                    match[f.name] = {'$regex' : fv, '$options' : 'i'}
                    break;
              }
        }
    }

    let data
    let groups = Object.keys(mapupdaterequestsQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = mapupdaterequestsQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }

    

    if (country != "_ALL_") {
        match._geoCountry = country
        if (state != "_ALL_") {
            match._geoState = state
            if (substate != "_ALL_") {
                match._geoSubState = substate
            }
        }
    }
    data = await db.collection('mapUpdateRequests').find(match).sort({_id:1}).toArray()
    
    console.log(JSON.stringify(match))
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < mapupdaterequestsQuerys.length; i++) {
    //     querys.push({label:mapupdaterequestsQuerys[i].label, value:i,  text:mapupdaterequestsQuerys[i].label, id:i});
    // }
    res.json(mapupdaterequestsQuerys);
}