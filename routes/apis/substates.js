
var exports = module.exports = {};


const SUBSTATES_FILTERS = [
]

const substatesQuerys = {
}

exports.filters = context => async function (req, res, next) {
    res.json(SUBSTATES_FILTERS);
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
    const subtates = await db.collection('tiles').distinct('substate',{updated_at:{$exists:true}})
    let match = {_id:{$in:subtates}}
    for (let index = 0; index < SUBSTATES_FILTERS.length; index++) {
        const f = SUBSTATES_FILTERS[index];
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
    let groups = Object.keys(substatesQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = substatesQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }

    


    // match = Object.assign(match,substatesQuerys[query].query);
    if (country != "_ALL_") {
        match.Country = country
        if (state != "_ALL_") {
            match.State = state
            if (substate != "_ALL_") {
                match.SubState = substate
            }
        }
        data = await db.collection('substate').find(match).sort({_id:1}).toArray()
    }else{
        data = await db.collection('substate').find(match).sort({_id:1}).toArray()
    }
    console.log(JSON.stringify(match))
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < substatesQuerys.length; i++) {
    //     querys.push({label:substatesQuerys[i].label, value:i,  text:substatesQuerys[i].label, id:i});
    // }
    res.json(substatesQuerys);
}