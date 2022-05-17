// Compute the mean for the window
from(bucket: "water_level_raw")
    |> range(start: params.start, stop: params.stop)
    |> mean()
    |> to(bucket: "water_level_mean_1h", timeColumn: "_stop")
    |> yield(name: "means")


// Compute and store new checksum for this window
from(bucket: "water_level_raw")
    |> range(start: params.start, stop: params.stop)
    |> group(columns: ["_measurement", "_field"])
    |> count()
    |> to(bucket: "water_level_checksum")
    |> yield(name: "checksums")

