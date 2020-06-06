
var exports = module.exports = {};

const USER_FILTERS = [
]

const usersQuerys = {
    "group 1": // Aqui iria el name que le quieres poner al grupo
        [
            {
                label: "ALL",
                query:{},
            },
        ],
};

exports.filters = context => async function (req, res, next) {
    res.json(USER_FILTERS);
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
        placeupdates=false
    } = req.body

    let match = {}
    for (let index = 0; index < USER_FILTERS.length; index++) {
        const f = USER_FILTERS[index];
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
    let groups = Object.keys(usersQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = usersQuerys[group]
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
                if (city != "_ALL_") {
                    match._WZcityName = city
                }
            }
        }
    }
    console.log(match)
    data = await db.collection('usuarios').find(match).sort({_id:1}).toArray()
    
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    res.json(usersQuerys);
}


exports.clean = context => async function(req, res, next) {
    try {
        const { db } = context


        let segments = await db.collection('segments').aggregate([ 
            {            
                $group:{
                    _id:"$updatedBy",
                    updatedOn_segments:{$max:"$updatedOn"},
                    segments: { $sum: 1 }
                }
            },
            { "$out" : "segments_by_user"}
        ]).toArray()
        let ids = (await db.collection('segments_by_user').find().project({_id:1}).toArray()).map(s=>s._id)

        let venues = await db.collection('venues').aggregate([ 
            {
                                
                $group:{
                    _id:"$updatedBy",
                    updatedOn_venues:{$max:"$updatedOn"},
                    venues: { $sum: 1 }
                }
            },
            { "$out" : "venues_by_user"}
        ]).toArray()
        ids = [...(await db.collection('venues_by_user').find().project({_id:1}).toArray()).map(s=>s._id), ...ids]
        // console.log(ids)


        const aggregate = await db.collection('users').aggregate([ 
            { $match : {"_id":{'$in':ids}} },
            {
                "$lookup":
                    {
                      from: "segments_by_user",
                       localField: "_id",
                       foreignField: "_id",
                      as: "segments",
                    }
            }, 
                                   {
                                     '$addFields':
                                       {
                                         segments : { $sum: "$segments.segments" },
                                         updatedOn_segments : { $sum: "$segments.updatedOn_segments" }
                                       }
                                   },
    {
        "$lookup":
            {
              from: "venues_by_user",
              localField: "_id",
              foreignField: "_id",
              as: "venues"
            }
    },
    {
         '$addFields':
           {
             venues : { $sum: "$venues.venues" },
             updatedOn_venues : { $sum: "$venues.updatedOn_venues" }
           }
       },
    //    { $match : {'$or':[{'venues': {$gt: 0}}, {'segments': {$gt: 0}}]} },
            { "$out" : "usuarios"}
        ], { "allowDiskUse": true } ).toArray()
        res.json({
            sucess:true
        });
    }catch(error){
        console.log(error)
        res.json({
            error
        });
    }
}