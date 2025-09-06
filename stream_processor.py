import json
import math
import time
import numpy as np
import redis
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

from config import (
    KAFKA_BOOTSTRAP, TICKS_TOPIC, NEWS_TOPIC,
    ROLLING_WINDOW_SEC, WATERMARK_SEC,
    MC_PATHS, MC_HORIZON_D, RISK_FREE,
    REDIS_HOST, REDIS_PORT, REDIS_DB
)
from sentiment import polarity
from mc_sim import gbm_mc

polarity_udf = F.udf(lambda s: float(polarity(s)), T.DoubleType())

def connect_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def write_snapshot(r, key, value):
    r.set(key, json.dumps(value))

def main():
    spark = (SparkSession.builder
             .appName("market-sim-stream")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    ticks_raw = (spark.readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                       .option("subscribe", TICKS_TOPIC)
                       .option("startingOffsets", "latest")
                       .load())

    ticks = (ticks_raw.select(F.col("value").cast("string").alias("json"))
                     .select(F.from_json("json", T.StructType()
                         .add("ts", T.StringType())
                         .add("symbol", T.StringType())
                         .add("price", T.DoubleType())).alias("data"))
                     .select("data.*")
                     .withColumn("event_time", F.to_timestamp("ts")))

    w_ticks = (ticks
               .withWatermark("event_time", f"{WATERMARK_SEC} seconds")
               .withColumn("log_price", F.log("price"))
               .withColumn("ret", F.log("price") - F.lag(F.log("price")).over(
                   Window.partitionBy("symbol").orderBy("event_time")
               )))

    roll_stats = (w_ticks
                  .groupBy(
                      F.window("event_time", f"{ROLLING_WINDOW_SEC} seconds"),
                      F.col("symbol")
                  )
                  .agg(
                      F.last("price").alias("last_price"),
                      F.stddev_pop("ret").alias("ret_std")
                  )
                  .withColumn("ret_std", F.coalesce(F.col("ret_std"), F.lit(0.0))))

    news_raw = (spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                      .option("subscribe", NEWS_TOPIC)
                      .option("startingOffsets", "latest")
                      .load())

    news = (news_raw.select(F.col("value").cast("string").alias("json"))
                    .select(F.from_json("json", T.StructType()
                        .add("ts", T.StringType())
                        .add("title", T.StringType())
                        .add("description", T.StringType())
                        .add("source", T.StringType())
                        .add("url", T.StringType())).alias("data"))
                    .select("data.*")
                    .withColumn("event_time", F.to_timestamp("ts"))
                    .withColumn("text", F.concat_ws(" ", "title", "description"))
                    .withColumn("sentiment", polarity_udf("text"))
                    .withWatermark("event_time", f"{WATERMARK_SEC} seconds"))

    sent_agg = (news.groupBy(F.window("event_time", f"{ROLLING_WINDOW_SEC} seconds"))
                     .agg(F.avg("sentiment").alias("sent_roll")))

    joined = (roll_stats.join(
                    sent_agg,
                    on=roll_stats["window"] == sent_agg["window"],
                    how="left")
                    .select(
                        roll_stats["window"].alias("w"),
                        "symbol", "last_price", "ret_std",
                        F.coalesce("sent_roll", F.lit(0.0)).alias("sent_roll"))
              )

    def foreach_batch(df, batch_id):
        r = connect_redis()
        rows = df.collect()
        snapshot = {"batch_id": int(batch_id), "updated": int(time.time()), "symbols": {}}

        for row in rows:
            symbol = row["symbol"]
            s0 = float(row["last_price"]) if row["last_price"] is not None else None
            ret_std = float(row["ret_std"]) if row["ret_std"] is not None else 0.0
            sent = float(row["sent_roll"]) if row["sent_roll"] is not None else 0.0

            if not s0 or math.isnan(s0) or s0 <= 0:
                continue

            daily_sigma = max(ret_std, 1e-6) * 16.0
            mu = RISK_FREE + max(min(sent, 0.02), -0.02)

            paths = gbm_mc(s0, mu, daily_sigma, days=MC_HORIZON_D, paths=MC_PATHS)
            end_dist = paths[-1, :]

            p05 = float(np.percentile(end_dist, 5))
            p50 = float(np.percentile(end_dist, 50))
            p95 = float(np.percentile(end_dist, 95))

            snapshot["symbols"][symbol] = {
                "s0": s0,
                "mu": mu,
                "sigma": daily_sigma,
                "sentiment": sent,
                "horizon_days": MC_HORIZON_D,
                "bands": {"p05": p05, "p50": p50, "p95": p95},
            }

        write_snapshot(r, "market:latest", snapshot)

    query = (joined.writeStream
                    .foreachBatch(foreach_batch)
                    .outputMode("update")
                    .trigger(processingTime="10 seconds")
                    .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()
