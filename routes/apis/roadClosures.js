
var exports = module.exports = {};

const ROADCLOSURES_FILTERS = [
]

const roadClosuresQuerys = {
    "General":[
        {
            label:"With Event",
            query:{eventId:{$ne:null}}
        },
    ]
}

exports.filters = context => async function (req, res, next) {
    res.json(ROADCLOSURES_FILTERS);
}

exports.list = context => async function(req, res, next) {
    const { db } = context
    const { 
        country="_ALL_",
        state="_ALL_",
        substate="_ALL_", 
        city="_ALL_", 
        query=0, 
        filter={},
        qid,
        qproperty
    } = req.body

    let _qid = !isNaN(qid)?`${parseInt(qid)}`.length === qid.length? parseInt(qid):qid:qid

    let match = {[qproperty]:_qid}
    if (country != "_ALL_") {
        match = {}
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

    for (let index = 0; index < ROADCLOSURES_FILTERS.length; index++) {
        const f = ROADCLOSURES_FILTERS[index];
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
    let groups = Object.keys(roadClosuresQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = roadClosuresQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }
    // match = Object.assign(match,roadClosures[query].query);
    

    // data = await db.collection('roadClosures').find(match).sort({_id:1}).toArray()
    data = await db.collection('roadClosures').aggregate([ 
        { $match : match },
        {
            "$lookup":
                {
                  from: "majorTrafficEvents",
                  localField: "eventId",
                  foreignField: "_id",
                  as: "majorTrafficEvents"
                }
        },
        { $sort : { _id:1 } }
        //{ $count: "passing_scores" }
    ], { "allowDiskUse": true } ).toArray()

    console.log(JSON.stringify(match))
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < roadClosuresQuerys.length; i++) {
    //     querys.push({label:roadClosuresQuerys[i].label, value:i,  text:roadClosuresQuerys[i].label, id:i});
    // }
    res.json(roadClosuresQuerys);
}