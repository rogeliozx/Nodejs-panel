
var exports = module.exports = {};

const CATEGORIES_VENUES = {
    'AIRPORT': 'Airport',
    'ART_GALLERY': 'Art Gallery',
    'ARTS_AND_CRAFTS': 'Arts & Crafts',
    'ATM': 'ATM',
    'BAKERY': 'Bakery',
    'BANK_FINANCIAL': 'Bank / Financial',
    'BAR': 'Bar',
    'BEACH': 'Beach',
    'BED_AND_BREAKFAST': 'Bed & Breakfast',
    'BOOKSTORE': 'Bookstore',
    'BRIDGE': 'Bridge',
    'BUS_STATION': 'Bus Station',
    'CAFE': 'Coffee shop',
    'CAMPING_TRAILER_PARK': 'Camping / Trailer Park',
    'CANAL': 'Canal',
    'CAR_DEALERSHIP': 'Car Dealership',
    'CAR_RENTAL': 'Car Rental',
    'CAR_SERVICES': 'Car services',
    'CAR_WASH': 'Car Wash',
    'CARPOOL_SPOT': 'Carpool Spot',
    'CASINO': 'Casino',
    'CEMETERY': 'Cemetery',
    'CHARGING_STATION': 'Charging Station',
    'CITY_HALL': 'City Hall',
    'CLUB': 'Club',
    'COLLEGE_UNIVERSITY': 'College / University',
    'CONSTRUCTION_SITE': 'Construction Site',
    'CONVENIENCE_STORE': 'Convenience Store',
    'CONVENTIONS_EVENT_CENTER': 'Conventions / Event Center',
    'COTTAGE_CABIN': 'Cottage / Cabin',
    'COURTHOUSE': 'Courthouse',
    'CULTURE_AND_ENTERTAINEMENT': 'Culture & entertainment',
    'CURRENCY_EXCHANGE': 'Currency Exchange',
    'DAM': 'Dam',
    'DEPARTMENT_STORE': 'Department Store',
    'DESSERT': 'Dessert',
    'DOCTOR_CLINIC': 'Doctor / Clinic',
    'ELECTRONICS': 'Electronics',
    'EMBASSY_CONSULATE': 'Embassy / Consulate',
    'EMERGENCY_SHELTER': 'Emergency Shelter',
    'FACTORY_INDUSTRIAL': 'Factory / Industrial',
    'FARM': 'Farm',
    'FASHION_AND_CLOTHING': 'Fashion and Clothing',
    'FAST_FOOD': 'Fast Food',
    'FERRY_PIER': 'Ferry Pier',
    'FIRE_DEPARTMENT': 'Fire Department',
    'FLOWERS': 'Flowers',
    'FOOD_AND_DRINK': 'Food and Drink',
    'FOOD_COURT': 'Food Court',
    'FOREST_GROVE': 'Forest / Grove',
    'FURNITURE_HOME_STORE': 'Furniture / Home Store',
    'GAME_CLUB': 'Game Club',
    'GARAGE_AUTOMOTIVE_SHOP': 'Garage / Automotive Shop',
    'GAS_STATION': 'Gas Station',
    'GIFTS': 'Gifts',
    'GOLF_COURSE': 'Golf Course',
    'GOVERNMENT': 'Government',
    'GYM_FITNESS': 'Gym / Fitness',
    'HARDWARE_STORE': 'Hardware Store',
    'HOSPITAL_URGENT_CARE': 'Hospital / Urgent Care',
    'HOSTEL': 'Hostel',
    'HOTEL': 'Hotel',
    'ICE_CREAM': 'Ice Cream',
    'INFORMATION_POINT': 'Information Point',
    'ISLAND': 'Island',
    'JEWELRY': 'Jewelry',
    'JUNCTION_INTERCHANGE': 'Junction / Interchange',
    'KINDERGARDEN': 'Kindergarten',
    'LAUNDRY_DRY_CLEAN': 'Laundry / Dry Clean',
    'LIBRARY': 'Library',
    'LODGING': 'Lodging',
    'MARKET': 'Market',
    'MILITARY': 'Military',
    'MOVIE_THEATER': 'Movie Theater',
    'MUSEUM': 'Museum',
    'MUSIC_STORE': 'Music Store',
    'MUSIC_VENUE': 'Music Venue',
    'NATURAL_FEATURES': 'Natural Features',
    'OFFICES': 'Offices',
    'ORGANIZATION_OR_ASSOCIATION': 'Organization or Association',
    'OTHER': 'Other',
    'OUTDOORS': 'Outdoors',
    'PARK': 'Park',
    'PARKING_LOT': 'Parking Lot',
    'PERFORMING_ARTS_VENUE': 'Performing Arts Venue',
    'PERSONAL_CARE': 'Personal Care',
    'PET_STORE_VETERINARIAN_SERVICES': 'Pet Store / Veterinarian Services',
    'PHARMACY': 'Pharmacy',
    'PHOTOGRAPHY': 'Photography',
    'PLAYGROUND': 'Playground',
    'PLAZA': 'Plaza',
    'POLICE_STATION': 'Police Station',
    'POOL': 'Pool',
    'POST_OFFICE': 'Post Office',
    'PRISON_CORRECTIONAL_FACILITY': 'Prison / Correctional Facility',
    'PROFESSIONAL_AND_PUBLIC': 'Professional and public',
    'PROMENADE': 'Promenade',
    'RACING_TRACK': 'Racing Track',
    'RELIGIOUS_CENTER': 'Religious Center',
    'RESIDENCE_HOME': 'Residence / Home',
    'REST_AREAS': 'Rest area',
    'RESTAURANT': 'Restaurant',
    'RIVER_STREAM': 'River / Stream',
    'SCENIC_LOOKOUT_VIEWPOINT': 'Scenic Lookout / Viewpoint',
    'SCHOOL': 'School',
    'SEA_LAKE_POOL': 'Sea / Lake / Pool',
    'SEAPORT_MARINA_HARBOR': 'Seaport / Marina / Harbor',
    'SHOPPING_AND_SERVICES': 'Shopping and services',
    'SHOPPING_CENTER': 'Shopping Center',
    'SKI_AREA': 'Ski Area',
    'SPORTING_GOODS': 'Sporting Goods',
    'SPORTS_COURT': 'Sports Court',
    'STADIUM_ARENA': 'Stadium / Arena',
    'SUBWAY_STATION': 'Subway Station',
    'SUPERMARKET_GROCERY': 'Supermarket / Grocery',
    'SWAMP_MARSH': 'Swamp / Marsh',
    'SWIMMING_POOL': 'Swimming Pool',
    'TAXI_STATION': 'Taxi Station',
    'TELECOM': 'Telecom',
    'THEATER': 'Theater',
    'THEME_PARK': 'Theme Park',
    'TOURIST_ATTRACTION_HISTORIC_SITE': 'Tourist Attraction / Historic Site',
    'TOY_STORE': 'Toy Store',
    'TRAIN_STATION': 'Train Station',
    'TRANSPORTATION': 'Transportation',
    'TRASH_AND_RECYCLING_FACILITIES': 'Trash & recycling facility',
    'TRAVEL_AGENCY': 'Travel Agency',
    'TUNNEL': 'Tunnel',
    'ZOO_AQUARIUM': 'Zoo / Aquarium'
}
const VENUE_FILTERS = [
    {
        label:"Categories",
        name:"categories",
        type:"select",
        options:CATEGORIES_VENUES
    },
    {
        label:"Categories",
        name:"categories",
        type:"buttons",
        options:CATEGORIES_VENUES
    },
    {
        label:"Approved",
        name:"approved",
        type:"boolean"
    },
    {
        label:"Name",
        name:"name",
        type:"text"
    }
]

const venuesQuerys = {
    "group 1": // Aqui iria el name que le quieres poner al grupo
        [
            {
                label: "ALL",
                query:{
            
                },
            },
            {
                label: "Venues without address",
                query:{
                    streetID : null
                },
            },
            {
                label: "Unnudged Venues",
                query:{
                    entryExitPoints : { $exists: false }
                },
            },
            {
                label: "Unnudged RPPs",
                query:{
                    entryExitPoints: { $exists: false },
                    categories: ["RESIDENCE_HOME"]
                },
            },
            {
                label: "Parking Lot Points",
                query:{
                    categories: ["PARKING_LOT"],
                    "geometry.type" : "Point"
                },
            },
            {
                label: "All Place Update Requests",
                query:{
                    "venueUpdateRequests": { $exists: true }
                },
            },
            {
                label: "Gas Stations No Brand",
                query:{
                    categories: ["GAS_STATION"],
                    brand: { $exists: false },
                    "approved": true
                },
            },
            {
                label: "Gas Station Points",
                query:{
                    categories: ["GAS_STATION"],
                    brand: { $exists: false },
                    "geometry.type" : "Point"
                },
            },
            {
                label: "Flagged Gas Stations",
                query:{
                    categories: ["GAS_STATION"],
                    "venueUpdateRequests.flagType": "WRONG_DETAILS"
                },
            },
        ],
        "Group 2":[
            {
                label: "Feed inserted venues",
                query:{
                    "venueUpdateRequests.createdBy": { $in: [ -5, 105774162, 361008095, 304740435 ] }
                },
            },
            {
                label: "Feed inserted Parking Lots",
                query:{
                    "venueUpdateRequests.createdBy": { $in: [ 338475699, 358739837 ] }
                },
            },
            {
                label: "Unbranded Gas Stations",
                query:{
                    categories: ["GAS_STATION"],
                    brand: "Unbranded"
                },
            },
            {
                label: "Navads Imports",
                query:{
                    "venueUpdateRequests.createdBy": { $in: [ 785557785 ] }
                },
            }
        ]
};

exports.filters = context => async function (req, res, next) {
    res.json(VENUE_FILTERS);
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
    let _qid = !isNaN(qid)?`${parseInt(qid)}`.length === qid.length? parseInt(qid):qid:qid

    let match = placeupdates?{ 'venueUpdateRequests.0': {$exists: true}, [qproperty]:_qid}:{[qproperty]:_qid}
    for (let index = 0; index < VENUE_FILTERS.length; index++) {
        const f = VENUE_FILTERS[index];
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
    let groups = Object.keys(venuesQuerys)
    for (let iGroup = 0; iGroup < groups.length; iGroup++) {
        const group = groups[iGroup];
        const groupArr = venuesQuerys[group]
        for (let iG = 0; iG < groupArr.length; iG++) {
            const q = groupArr[iG];
            if (q.label === query) {
                match = Object.assign(match, q.query)
            }
        }
    }
    // match = Object.assign(match, venuesQuerys[query].query);
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
    data = await db.collection('venues').find(match).sort({_id:1}).toArray()
    
    res.json({
        data
    });
}

exports.querys = context => async function (req, res, next) {
    // var querys = [];
    // for (var i = 0; i < venuesQuerys.length; i++) {
    //     querys.push({label:venuesQuerys[i].label, value:i, text:venuesQuerys[i].label, id:i});
    // }
    res.json(venuesQuerys);
}