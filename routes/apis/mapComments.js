
var exports = module.exports = {};

const MAPCOMMENTS_FILTERS = [
]

const mapCommentsQuerys = {
  
};

exports.filters = context => async function (req, res, next) {
    res.json(MAPCOMMENTS_FILTERS);
};

exports.list = context => async function(req, res, next) {
    const { db } = context
    const { 
        country="_ALL_",
        state="_ALL_",
        substate="_ALL_", 
        city="_ALL_", 
        
        query=0,
        filter={},
        placeupdates=false,
        qid,
        qproperty
    } = req.body

    let match = {[qproperty]:qid}
    for (let index = 0; index < MAPCOMMENTS_FILTERS.length; index++) {
        const f = MAPCOMMENTS_FILTERS[index];
        const fv = filter[f.name]
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
    let groups = Object.keys(mapCommentsQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = mapCommentsQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }
    // match = Object.assign(match, mapCommentsQuerys[query].query);
    if (country != "_ALL_") {
        match._geoCountry = country
        if (state != "_ALL_") {
            match._geoState = state
            if (substate != "_ALL_") {
                match._geoSubState = substate
                if (city != "_ALL_") {
                    match._WZcityName = city
                }
            }
        }
    }
    console.log(match)
    data = await db.collection('mapComments').find(match).sort({_id:1}).toArray()
    
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < mapCommentsQuerys.length; i++) {
    //     querys.push({label:mapCommentsQuerys[i].label, value:i, text:mapCommentsQuerys[i].label, id:i});
    // }
    res.json(mapCommentsQuerys);
}