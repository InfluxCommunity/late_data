import "influxdata/influxdb/secrets"
import "experimental/http/requests"
import "json"
import "date"
import "experimental"
import "slack"
import "regexp"
import "influxdata/influxdb/schema"
import "influxdata/influxdb/monitor"

option task = {name: "water_level_checksum", every: 5m, offset: 1m}

token = secrets.get(key: "token")

// Notification and check creation (optional)
check = {_check_id: "i60mkgh05555", _check_name: "Late Data Check", _type: "custom", tags: {}}
notification = {
    _notification_rule_id: "i60mkgh05555",
    _notification_rule_name: "Late Data Check",
    _notification_endpoint_id: "i60mkgh05556",
    _notification_endpoint_name: "Late Data Check Endpoint",
}

// trigger for alert. Only needs to check a value exists (optional)
trigger = (r) => exists r["_value"] 

// Message template for alert (optional)
messageFn = (r) =>
"WARNING: Late arriving data.
Details:
Measurment: ${r.field}
Time: ${r._time}
Old Count:  ${r.old_count}
New Count : ${r.new_count}
Script trigger status:  ${r.code}"

// invokeScript calls a Flux script with the given start stop
// parameters to recompute the window.
invokeScript = (start, stop) =>
    requests.post(
        // We have hardcoded the script ID here
        url: "https://us-east-1-1.aws.cloud2.influxdata.com/api/v2/scripts/0983182ad3ca1000/invoke",
        headers: ["Authorization": "Token ${token}", "Accept": "application/json", "Content-Type": "application/json"],
        body: json.encode(v: {params: {start: string(v: start), stop: string(v: stop)}}),
    )

// Only query windows that span a full hour
start = date.truncate(t: -1d, unit: 1h)
stop = date.truncate(t: now(), unit: 1h)

newCounts =
    from(bucket: "water_level_raw")
        |> range(start: start, stop: stop)
        |> group(columns: ["_measurement", "_field"])
        |> aggregateWindow(every: 1h, fn: count)

// Always compute the most recent interval
newCounts
    |> filter(fn: (r) => r._time == stop)
    |> map(
        fn: (r) => {
            response = invokeScript(start: date.sub(d: 1h, from: r._time), stop: r._time)
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
            response = invokeScript(start: date.sub(d: 1h, from: r._time), stop: r._time)

            return {r with code: response.statusCode}
        },
    )
    |> yield(name: "diffs")
    // Add alert to notify user on late arriving data (optional)
    |> last()
    |> rename(columns: {"_field":"field"})
    |> monitor["check"](data: check, messageFn: messageFn, crit: trigger)
    |> filter(fn: trigger)
    |> monitor["notify"](
        data: notification,
        endpoint:
            slack["endpoint"](url: "https://hooks.slack.com/services/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")(
                mapFn: (r) => ({channel: "#notifications-testing", text: "${r._message}", color: "#DC4E58"}),
            ),
    )