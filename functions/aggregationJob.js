exports = async function() {
    const db = context.services
        .get("mongodb-atlas")
        .db("market_data");

    const bars1m = db.collection("bars_1m");

    console.log("Daily aggregation started");

    // aggregation code here

    console.log("Daily aggregation finished");

    return "OK";
};