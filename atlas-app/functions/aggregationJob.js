exports = async function() {
    
    const db = context.services
        .get("stox")
        .db("stock_data");

    const bars1m = db.collection("minute_bars");

    console.log("Daily aggregation started");

    // aggregation code here

    console.log("Daily aggregation finished");

    return "OK";
};