import "influxdata/influxdb/secrets"
import "experimental/http/requests"
import "json"
import "date"
import "experimental"

option task = {name: "water_level_checksum", every: 1m, offset: 10s}

// Size of the window to aggregate
every = task.every

// Longest we are willing to wait for late data
late_window = 1h

token = secrets.get(key: "SELF_TOKEN")

// invokeScript calls a Flux script with the given start stop
// parameters to recompute the window.
invokeScript = (start, stop) =>
    requests.post(
        // We have hardcoded the script ID here
        url: "https://eastus-1.azure.cloud2.influxdata.com/api/v2/scripts/095fabd404108000/invoke",
        headers: ["Authorization": "Token ${token}", "Accept": "application/json", "Content-Type": "application/json"],
        body: json.encode(v: {params: {start: string(v: start), stop: string(v: stop)}}),
    )

// Only query windows that span a full minute
start = date.truncate(t: -late_window, unit: every)
stop = date.truncate(t: now(), unit: every)

newCounts =
    from(bucket: "water_level_raw")
        |> range(start: start, stop: stop)
        |> group(columns: ["_measurement", "_field"])
        |> aggregateWindow(every: every, fn: count)

// Always compute the most recent interval
newCounts
    |> filter(fn: (r) => r._time == stop)
    |> map(
        fn: (r) => {
            response = invokeScript(start: date.sub(d: every, from: r._time), stop: r._time)

            return {r with code: response.statusCode}
        },
    )
    |> yield(name: "current")

oldCounts =
    from(bucket: "water_level_checksum")
        |> range(start: start, stop: stop)
        |> group(columns: ["_measurement", "_field"])

// Compare old and new checksum
// TODO: Use outer join when its available
experimental.join(
    left: oldCounts,
    right: newCounts,
    fn: (left, right) => ({left with old_count: left._value, new_count: right._value}),
)
    // Recompute any windows where the checksum is different
    |> filter(fn: (r) => r.old_count != r.new_count)
    |> map(
        fn: (r) => {
            response = invokeScript(start: date.sub(d: every, from: r._time), stop: r._time)

            return {r with code: response.statusCode}
        },
    )
    |> yield(name: "diffs")