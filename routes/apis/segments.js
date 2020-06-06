
var exports = module.exports = {};

const ROADTYPES_SEGMENTS = {
    '1':'Local Street',
    '2':'Primary Street',
    '3':'Freeway (Interstate / Other)',
    '4':'Ramp',
    '5':'Routable Pedestrian Path',
    '6':'Major Highway',
    '7':'Minor Highway',
    '8':'Off-road / Not maintained',
    '10':'Non-Routable Pedestrian Path',
    '15':'Ferry',
    '16':'Stairway',
    '17':'Private Road',
    '18':'Railroad',
    '19':'Runway/Taxiway',
    '20':'Parking Lot Road',
    '22':'Passageway',	
}

const SEGMENTS_FILTERS = [
    {
        label:"Road Type",
        name:"roadType",
        type:"select",
        options:ROADTYPES_SEGMENTS,
        number:true
    },
    {
        label:"Road Type",
        name:"roadType",
        type:"buttons",
        options:ROADTYPES_SEGMENTS,
        number:true
    },
    {
        label:"Has Closures",
        name:"hasClosures",
        type:"boolean"
    },
    {
        label:"Street Name",
        name:"_WZstreetName",
        type:"text"
    }
]

const segmentsQuerys = {
    "Disconnected Segments":[
        {
            label:"Two Way",
            query:{ roadType: { $in: [ 3, 4, 6, 7, 1, 2, 8, 17, 20, 15, 22 ] }, $and:[ { "revDirection" : true,  "fwdDirection" : true,  "error_fwdDirection" : true, "error_revDirection" : true } ] }
        },
        {
            label:"One Way A to B",
            query:{ roadType: { $in: [ 3, 4, 6, 7, 1, 2, 8, 17, 20, 15, 22 ] }, $and: [ { "revDirection" : false,  "fwdDirection" : true, "error_fwdDirection" : true } ] }
        },
        {
            label:"One Way B to A",
            query:{ roadType: { $in: [ 3, 4, 6, 7, 1, 2, 8, 17, 20, 15, 22 ] }, $and: [ { "revDirection" : true,  "fwdDirection" : false,  "error_revDirection" : true } ] }
        },
        {
            label:"Unknown Direction",
            query:{ $and: [ { "roadType": { $in: [ 3, 4, 6, 7, 1, 2, 8, 17, 20, 15, 22 ] }, "fwdDirection": false, "revDirection": false, } ] }  ,
        },
        {
            label:"Two Way (ALL) Highways",
            query:{ roadType: { $in: [ 3, 4, 6, 7 ] }, $and:[ { "revDirection" : true,  "fwdDirection" : true,  "error_fwdDirection" : true, "error_revDirection" : true } ] }
        },
        {
            label:"Two Way (All) Local roads",
            query:{ roadType: { $in: [ 2, 1, 22 ] }, $and:[ { "revDirection" : true,  "fwdDirection" : true,  "error_fwdDirection" : true, "error_revDirection" : true } ] }
        },
        {
            label:"Two Way Other drivable",
            query:{ roadType: { $in: [ 8, 20, 17, 15 ] }, $and:[ { "revDirection" : true,  "fwdDirection" : true,  "error_fwdDirection" : true, "error_revDirection" : true } ] }
        },
    ],
    "Problems":[
        {
            label:"Red roads",
            query:{ "primaryStreetID" : null }
        },
        {
            label:"Short segments (-5m)",
            query:{ length: { $lt: 5 },  junctionID: null} ,
        },
        {
            label:"Wrong elevation",
            query:{"level": { $in: [ -4, -3, 3, 4, 5, 6, 7, 8, 9 ] } } ,
        }
    ],
    "Attributes":[
        {
            label:"Irregular toll roads",
            query:{"roadType": { $in: [ 1, 2, 5, 8, 10, 15, 16, 17, 18, 19, 20, 22 ] }, $or: [ { "fwdToll": true }, { "revToll": true } ] } ,
        },
        {
            label:"No speed limits",
            query:{"roadType": { $in: [ 1, 2, 3, 4, 6, 7, 8, 15 ] }, $or: [ { "revMaxSpeed": null, "revDirection": true}, { "fwdMaxSpeed": null, "fwdDirection": true } ] } ,
        },
        {
            label:"Unverified speed limits",
            query:{ $or: [ { "fwdDirection": true, "fwdMaxSpeedUnverified": true }, { "revDirection": true, "revMaxSpeedUnverified": true } ] } ,
        },
        {
            label:"Outdated Restrictions",
            query:{ "restrictions.timeFrames.endDate":{ $lte : (new Date()).toISOString().split("T")[0] } },
        },
    ]
}

exports.filters = context => async function (req, res, next) {
    res.json(SEGMENTS_FILTERS);
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
    for (let index = 0; index < SEGMENTS_FILTERS.length; index++) {
        const f = SEGMENTS_FILTERS[index];
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
    let groups = Object.keys(segmentsQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = segmentsQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }
    // match = Object.assign(match,segmentsQuerys[query].query);
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
        data = await db.collection('segments').find(match).sort({_id:1}).toArray()
    }else{
        data = await db.collection('segments').find(match).sort({_id:1}).toArray()
    }
    console.log(JSON.stringify(match))
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < segmentsQuerys.length; i++) {
    //     querys.push({label:segmentsQuerys[i].label, value:i,  text:segmentsQuerys[i].label, id:i});
    // }
    res.json(segmentsQuerys);
}