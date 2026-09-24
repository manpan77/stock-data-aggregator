function getBucketExpression(targetTimeframe, timezone = "UTC", date = "$timestamp") {
  switch (targetTimeframe) {
    case "5m":
      return {
        $dateTrunc: {
          date,
          unit: "minute",
          binSize: 5,
          timezone: "UTC"
        }
      };

    case "10m":
      return {
        $dateTrunc: {
          date,
          unit: "minute",
          binSize: 10,
          timezone: "UTC"
        }
      };

    case "15m":
      return {
        $dateTrunc: {
          date,
          unit: "minute",
          binSize: 15,
          timezone: "UTC"
        }
      };

    case "1h":
      return {
        $dateTrunc: {
          date,
          unit: "hour",
          binSize: 1,
          timezone: "UTC"
        }
      };

    case "2h":
      return {
        $dateTrunc: {
          date,
          unit: "hour",
          binSize: 2,
          timezone: "UTC"
        }
      };

    case "4h":
      return {
        $dateTrunc: {
          date,
          unit: "hour",
          binSize: 4,
          timezone: "UTC"
        }
      };

    case "1d":
      return {
        $dateTrunc: {
          date,
          unit: "day",
          timezone
        }
      };

    case "1w":
      return {
        $dateTrunc: {
          date,
          unit: "week",
          timezone,
          startOfWeek: "monday"
        }
      };

    case "1mo":
      return {
        $dateTrunc: {
          date,
          unit: "month",
          timezone
        }
      };

    default:
      throw new Error(`Unsupported target timeframe: ${targetTimeframe}`);
  }
}

function calculateCutoff(age, unit) {
  const date = new Date();

  switch (unit) {
    case "minute":
      date.setMinutes(date.getMinutes() - age);
      break;

    case "hour":
      date.setHours(date.getHours() - age);
      break;

    case "day":
      date.setDate(date.getDate() - age);
      break;

    case "month":
      date.setMonth(date.getMonth() - age);
      break;

    case "year":
      date.setFullYear(date.getFullYear() - age);
      break;

    default:
      throw new Error(`Unsupported age unit: ${unit}`);
  }

  return date;
}

async function aggregate(
  db,
  sourceTimeframe,
  targetTimeframe,
  age,
  ageUnit
) {
  const collection = db.collection("stock_bars");

  const cutoff = calculateCutoff(age, ageUnit);
  const usesExchangeTimezone = ["1d", "1w", "1mo"].includes(targetTimeframe);
  const timezone = usesExchangeTimezone ? "$tickerTimezone" : "UTC";
  const bucketCutoff = getBucketExpression(targetTimeframe, timezone, cutoff);

  const timezoneStages = usesExchangeTimezone ? [{
    $lookup: {
      from: "ticker_data",
      localField: "ticker",
      foreignField: "ticker",
      as: "tickerMetadata"
    }
  }, {
    $set: {
      tickerTimezone: {
        $ifNull: [
          { $arrayElemAt: ["$tickerMetadata.timezone", 0] },
          "UTC"
        ]
      }
    }
  }] : [];

  const completeBucketMatch = {
    $match: {
      $expr: {
        $lt: [
          "$bucketTimestamp",
          bucketCutoff
        ]
      }
    }
  };

  await collection.aggregate([
    {
      $match: {
        timeframe: sourceTimeframe,
        timestamp: { $lt: cutoff }
      }
    },

    ...timezoneStages,

    {
      $set: {
        bucketTimestamp: getBucketExpression(targetTimeframe, timezone)
      }
    },

    completeBucketMatch,

    {
      $sort: {
        ticker: 1,
        timestamp: 1
      }
    },

    {
      $group: {
        _id: {
          ticker: "$ticker",
          timestamp: "$bucketTimestamp"
        },

        open: { $first: "$open" },
        high: { $max: "$high" },
        low: { $min: "$low" },
        close: { $last: "$close" },
        volume: { $sum: { $ifNull: ["$volume", 0] } }
      }
    },

    {
      $project: {
        _id: 0,
        ticker: "$_id.ticker",
        timeframe: targetTimeframe,
        timestamp: "$_id.timestamp",
        open: 1,
        high: 1,
        low: 1,
        close: 1,
        volume: 1
      }
    },

    {
      $merge: {
        into: "stock_data",
        on: ["ticker", "timeframe", "timestamp"],
        whenMatched: "replace",
        whenNotMatched: "insert"
      }
    }
  ]).toArray();

  const sourceDocumentsToDelete = await collection.aggregate([
    {
      $match: {
        timeframe: sourceTimeframe,
        timestamp: { $lt: cutoff }
      }
    },

    ...timezoneStages,

    {
      $set: {
        bucketTimestamp: getBucketExpression(targetTimeframe, timezone)
      }
    },

    completeBucketMatch,

    {
      $project: {
        _id: 1
      }
    }
  ]).toArray();

  if (sourceDocumentsToDelete.length > 0) {
    await collection.deleteMany({
      _id: {
        $in: sourceDocumentsToDelete.map(({ _id }) => _id)
      }
    });
  }
}

exports = async function() {
    
    const db = context.services
        .get("stox")
        .db("stock_data");

    console.log("Daily aggregation started");

    await aggregate(db, "1m",  "5m",  1, "day");
    await aggregate(db, "5m",  "10m", 3, "day");
    await aggregate(db, "10m", "15m", 5, "day");
    await aggregate(db, "15m", "1h",  1, "month");
    await aggregate(db, "1h",  "2h",  3, "month");
    await aggregate(db, "2h",  "4h",  4, "month");
    await aggregate(db, "4h",  "1d",  6, "month");
    await aggregate(db, "1d",  "1w",  2, "year");
    await aggregate(db, "1w",  "1mo", 5, "year");

    console.log("Daily aggregation finished");

    return "OK";
};