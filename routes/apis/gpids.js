
var exports = module.exports = {};

exports.list = context => async function(req, res, next) {
    const { page=1 } = req.body
    const { db } = context

    const limit = 25  // repeated Gpids per page
    const count = await db.collection('gpids').find({"venues.1":{$exists:true}}).count()
    const data = await db.collection('gpids').find({"venues.1":{$exists:true}}).sort({_id:1}).skip((page-1)*limit).limit(limit).toArray()

    res.json({
        limit,
        page,
        count,
        data
    });
}
exports.listReact = context => async function(req, res, next) {
    const { db } = context
    const { 
        country="_ALL_",
        state="_ALL_",
        substate="_ALL_", 
    } = req.body
    let data, match = {}
    if (country != "_ALL_") {
        match._geoCountry = country
        if (state != "_ALL_") {
            match._geoState = state
            if (substate != "_ALL_") {
                match._geoSubState = substate
            }
        }
        let query = {venues:{ $elemMatch: match }}
        data = await db.collection('gpids').find(query).sort({_id:1}).toArray()
    }else{
        data = await db.collection('gpids').find().sort({_id:1}).toArray()
    }
    
    res.json({
        data
    });
}

exports.clean = context => async function(req, res, next) {
    try {
        const { db } = context
        const aggregate = await db.collection('venues').aggregate([ 
            { "$unwind" : "$externalProviderIDs" },
            { "$group" : { "_id" : "$externalProviderIDs", "count" :{ "$sum" : 1 }, 
                "venues": { $push: "$$ROOT" }, 
            }, },
            { "$match" : { "count" : { "$gt" : 1 } } },
            { $out : "gpids"}
            //{ $count: "passing_scores" }
        ], { "allowDiskUse": true } ).toArray()
        // console.log(aggregate)
        res.json({
            sucess:true
        });
    }catch(error){
        res.json({
            error
        });
    }
}