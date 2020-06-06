
var exports = module.exports = {};


const MAJORTRAFFICEVENTS_FILTERS = [
]

const majorTrafficEventsQuerys = {
}

exports.filters = context => async function (req, res, next) {
    res.json(MAJORTRAFFICEVENTS_FILTERS);
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
    for (let index = 0; index < MAJORTRAFFICEVENTS_FILTERS.length; index++) {
        const f = MAJORTRAFFICEVENTS_FILTERS[index];
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
    let groups = Object.keys(majorTrafficEventsQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = majorTrafficEventsQuerys[group]
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
    data = await db.collection('majorTrafficEvents').aggregate([
        { $match : match },
        {
          $lookup:
            {
              from: "cities",
              localField: "cityID",
              foreignField: "_id",
              as: "city"
            }
       },
       {
            $lookup:
            {
                from: "roadClosures",
                localField: "_id",
                foreignField: "eventId",
                as: "roadClosures"
            }
        },
        {
            $lookup:
            {
                from: "users",
                localField: "partners",
                foreignField: "_id",
                as: "partners_docs"
            }
        },
       { $sort : { _id:1 } }
     ])
    //  find(match).sort({_id:1})
     .toArray()
    
    console.log(JSON.stringify(match))
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    res.json(majorTrafficEventsQuerys);
}