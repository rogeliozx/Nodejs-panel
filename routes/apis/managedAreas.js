
var exports = module.exports = {};


exports.list = context => async function(req, res, next) {
    const { db } = context
    const { 
        userId=null,
    } = req.body
    let match={}
    if (userId !== null) {
        match={userID:parseInt(userId)}
    }
    data = await db.collection('managedAreas').find(match).limit(10).toArray()
    
    console.log(JSON.stringify(match))
    res.json({
        data
    });
}
