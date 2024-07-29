# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
from copy import deepcopy
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import logging
import re

from pathlib import Path
import sys

from ..converter import Converter

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

from common.grafana_model import GrafanaDashboard, GrafanaPanel
from common.influx_detection import AdvancedInfluxDetection
from common.error_manager import ErrorManager

LOG = logging.getLogger(__name__)

SCRAPE_INTERVAL_SECONDS = 30
OVER_TIME_AGGREGATIONS = [
    "avg",
    "min",
    "max",
    "sum",
    "quantile",
    "stddev",
    "stdvar",
    "count",
]
LABEL_COMPARISON_OPERATORS = ["=", "!=", "=~", "!~"]
LABEL_CONDITIONS_OPERATORS = ["<", ">", "<=", ">=", "="]
TIME_INTERVAL_REGEX = "[0-9]+[mhdwy][s]?"  # Match time(<<interval>>)
INVALID_PROMQL_METRIC_CHARACTERS = [".", "-", " ", "%"]

AGGREGATION_MAP = {
    "count": "count",
    "max": "max",
    "min": "min",
    "median": "avg",
    # Prometheus does not have a median function. It can do quantile(0,5...) on histograms only
    "mean": "avg",
    "non_negative_derivative": "rate",
    "non_negative_difference": "increase",
    "percentile": "avg",  # Percentile not supported except for histograms
    "stddev": "stddev",
    "sum": "sum",
    # Last isn't supported by Prometheus queries but if we drop the aggregation altogether there won't be
    # expected time based grouping so use average instead as an approximation
    # last_over_time is supported in Prometheus 2.25+ but not in M3 1.1 (at least)
    "last": "avg",
    # moving_average translated to avg_over_time
    "moving_average": "avg",
    "derivative": "rate",
    "difference": "delta",
}

# convert count(x) to sum(count_over_time(x[interval/__range]))
AGGREGATION_TOPLEVEL_MAP = {
    "count": "sum",
}

NONDEFAULT_METRIC_PREFIX_SCRAPE_INTERVAL_SECONDS = {
    "busy_metric_": 1,  # fictional example
}

# avg is shitty aggregation. So provide regexps for overrides, which
# we use if the original has 'avg' or n/a aggregation (but we do not
# override others).
METRIC_AGGREGATION_REGEXPS = [
    # These values we want to be as small as possible
    (".*_remaining_(percent_|)value", "min"),
    ("(disk|swap)_free", "min"),
    # ("cpu_usage_idle", "min"), # bad idea unless we applied to totalcpu only
    ("mem_available", "min"),
    # These ones as large as possible
    (".*_error_value", "max"),
    (".*_size(_value|$)", "max"),
    ("disk_used.*", "max"),
]

# Add cases that are not detected properly
TEMPLATING_METRIC_STATIC_CONVERSION = {}


def _duration_to_seconds(v: str) -> int:
    v = v.lower()
    if v.endswith("s"):
        return int(v[:-1])
    if v.endswith("m"):
        return int(v[:-1]) * 60
    if v.endswith("h"):
        return int(v[:-1]) * 3600
    if v.endswith("d"):
        return int(v[:-1]) * 86400
    if v.endswith("w"):
        return int(v[:-1]) * 86400 * 7
    raise NotImplementedError(f"unknown duration {v}")


def _seconds_to_duration(seconds: int) -> str:
    mins, div = divmod(seconds, 60)
    if div or not mins:
        return f"{seconds}s"
    hours, div = divmod(mins, 60)
    if div or not hours:
        return f"{mins}m"
    return f"{hours}h"


class GroupBy(NamedTuple):
    group_by: str
    over_time: str
    fills: List[str]


class ConvertError(Exception):
    pass


class InfluxQLToM3DashboardConverter(Converter):

    def __init__(
        self,
        *,
        error_manager: ErrorManager,
        global_shared_state: dict,
        datasource_map: Optional[Dict[Any, str]] = None,
        alert_notifications_map: Optional[Dict[int, str]] = None,
        alert_notifications_uid_map: Optional[Dict[str, str]] = None,
        scrape_interval: int = SCRAPE_INTERVAL_SECONDS,
        replacement_datasource: dict = None,
        log_level=logging.INFO,
    ) -> None:
        super().__init__(__name__, global_shared_state, log_level, error_manager)

        self.datasource_map = datasource_map
        self.alert_notifications_map = alert_notifications_map
        self.alert_notifications_uid_map = alert_notifications_uid_map
        self.scrape_interval = scrape_interval
        self.group_by_labels = None
        self.metric_to_objects = (
            {}
        )  # dict of: metric -> {panel,dashboard title} to avoid iterating over all panels
        self._current_dashboard = ""
        self.replacement_datasource = replacement_datasource
        assert (
            "uid" in self.replacement_datasource
            and "type" in self.replacement_datasource
        ), "Datasource must be dict with uid and title keys"
        # (Flag, metric and parent json of label_values string) object  to determine if the metric in the label_values requires replacement. i.e:
        # label_values(net,host) ---> label_values(net__bytes_recv,host). Any metric with the same service
        # is acceptable.
        self._get_rep_metric = []
        self._context_templating = None
        self._influx_detector = AdvancedInfluxDetection(self.global_shared_state)

    def get_metric_field_from_select(self, select):
        field = None
        for item in select[0]:
            if item["type"] == "field":
                field = item["params"][0]
                break
        return field

    def _replace_invalid_metric_characters(self, metric) -> str:
        for character in INVALID_PROMQL_METRIC_CHARACTERS:
            metric = metric.replace(character, "_")
        return metric

    def add_to_metric_and_object_list(
        self, old_targets, new_targets, old_panel_datasource
    ):
        for i, old_target in enumerate(old_targets):
            if not self._influx_detector.is_target_influx(
                old_target, self._context_templating, old_panel_datasource
            ):
                self._error_manager.add_error(
                    "Skipping target - not Influx or is Flux",
                    error_level="DEBUG",
                    notes=(
                        f'target={old_target["refId"]}'
                        if "refId" in old_target
                        else "target unknown"
                    ),
                )
                continue
            if "rawQuery" in old_target and old_target["rawQuery"] is True:
                metric_name, field_name = self.get_metric_name(old_target["query"])
                metric_name += field_name
            else:
                metric_name = ""
                try:
                    metric_name = old_target["measurement"] + "_"
                except Exception as e:
                    self._error_manager.add_error(
                        "Missing measurement field", notes=str(old_target)
                    )
                    raise e
                # replace . and - with _ for the metric names. (promql valid metric name contains _ only)
                if field_name := self.get_metric_field_from_select(
                    old_target["select"]
                ):
                    metric_name += field_name
                else:
                    self._error_manager.add_error(
                        "Missing field name", notes=str(old_target)
                    )
                    raise ValueError("Missing field name")
            metric_name = self._replace_invalid_metric_characters(metric_name)
            try:
                self.metric_to_objects[metric_name][self._current_dashboard].append(
                    new_targets[i]
                )
            except KeyError:
                if not self.metric_to_objects.get(metric_name):
                    self.metric_to_objects[metric_name] = {}
                self.metric_to_objects[metric_name][self._current_dashboard] = []
            self.metric_to_objects[metric_name][self._current_dashboard].append(
                new_targets[i]
            )

    def get_metric_aggregation(self, metric_name):
        for metric_re, fun in METRIC_AGGREGATION_REGEXPS:
            if re.match(metric_re, metric_name) is not None:
                return fun
        return "avg"

    def convert_templating(self, template: dict) -> None:
        to_be_deleted = []
        for idx, item in enumerate(template["list"]):
            if item["type"] == "query":
                if self.replacement_datasource:
                    item["datasource"] = self.replacement_datasource
                elif self.datasource_map:
                    datasource = item["datasource"]
                    # Recent grafana -> hope it is ok
                    if isinstance(datasource, dict):
                        continue
                    item["datasource"] = self.datasource_map.get(datasource, datasource)
                item["sort"] = 3  # Numerical(asc)
                query = item["query"]
                original_query = item["query"]
                # Uncommon case where they query is a dict
                if isinstance(query, dict) and query.get("query"):
                    query = query["query"]

                # DG - Skip invalid stuff
                if query.lower().startswith("show field keys"):
                    to_be_deleted.append(idx)
                    continue

                pattern = r"SHOW TAG VALUES FROM ['|\"]?(?P<tag>.+?)['|\"]? WITH KEY\s?=\s?['|\"\ ](?P<key>.+?)(?:['|\"\ ]|$)"
                m = re.search(pattern, query, re.IGNORECASE)
                if m is None:
                    # check dashboard for alternative definition
                    pattern = r"SHOW TAG VALUES WITH KEY\s?=\s?['|\"\ ](?P<key>.*?)(?:['|\"\ ]|$)"
                    m = re.search(pattern, query, re.IGNORECASE)

                if m is None:
                    # Regex
                    pattern = r"show tag values from (?P<tag>.+?) with key\s?=~\s+\/(?P<key>.+?)\/"
                    m = re.search(pattern, query, re.IGNORECASE)
                    self._error_manager.add_error(
                        "REGEXP show tag values is not supported in Prometheus, converting to STATIC",
                        error_level="WARN",
                    )

                if m is None:
                    raise ValueError(f"Unable to find tag values or key from {query!r}")

                groupdict = m.groupdict()
                metric = groupdict.get("tag")
                key = groupdict["key"]

                key = self._replace_invalid_metric_characters(key)
                if metric:
                    metric = self._replace_invalid_metric_characters(metric)
                    if metric.startswith("/"):
                        metric = metric.replace("/", "")

                    if metric.lower().find(".*") != -1:
                        metric = metric.replace(".*", "")
                        metric = metric.replace("(", "").replace(")", "")
                # This is for this cases we seem not to find a match in processor often, so we statically assign - DG
                if metric in TEMPLATING_METRIC_STATIC_CONVERSION.keys():
                    metric = TEMPLATING_METRIC_STATIC_CONVERSION[metric]
                if metric:
                    # This produces 100% valid query but because of metric names processor we don't need that 90% of time. And is hella slower - DG
                    # new_query = 'label_values({{__name__=~"{}.*"}},{})'.format(metric, key)
                    metric = metric.replace("/", "")
                    new_query = f"label_values({metric},{key})"
                else:
                    new_query = f"label_values({key})"

                item["query"] = new_query
                item["definition"] = new_query

                # Fix templating being destroyed by PromQL convention or just broken
                # from the begining - DG
                if "allValue" in item and item["allValue"] == "*":
                    item["allValue"] = ".*"
                if metric:
                    self._get_rep_metric.append((True, metric, item))
            elif item["type"] in {"custom", "interval", "datasource"}:
                pass
            else:
                raise ValueError(f"Unknown template type for item {item}")
        for idx in sorted(to_be_deleted, reverse=True):
            del template["list"][idx]

    def convert_alerts(self, alert: dict, panel_title) -> None:
        if not self.alert_notifications_map or not self.alert_notifications_uid_map:
            del alert["notifications"]
            LOG.warning(
                f"No alert notification mapping defined, deleting alert notifications array in panel: {panel_title}"
            )
            return
        alert["name"] = "{name} (M3)".format(name=alert["name"])
        notifications: List[dict] = []
        for notification in alert["notifications"]:
            n_id = notification.get("id")
            new_uid = self.alert_notifications_map.get(n_id) if n_id else None
            if new_uid is None:
                n_uid = notification["uid"]
                try:
                    new_uid = self.alert_notifications_map.get(int(n_uid))
                except ValueError:
                    new_uid = self.alert_notifications_uid_map.get(n_uid)
            if new_uid is None:
                raise ValueError(
                    f"Alert mapping for notification {notification} missing"
                )
            new_notification = {"uid": new_uid}
            if new_notification not in notifications:
                notifications.append(new_notification)
        alert["notifications"] = notifications

    def extract_key_all_case(self, query):
        pattern = r"SHOW TAG VALUES WITH KEY\s?=\s?['|\"](?P<key>.*?)['|\"]"
        m = re.search(pattern, query)
        if not m:
            pattern = r"show tag values with key\s?=\s?['|\"](?P<key>.*?)['|\"]"
            m = re.search(pattern, query)
        return m

    def format_expression(
        self,
        *,
        divide_by_self: bool,
        metric_name: str,
        aggregations: List[str],
        labels: str,
        group_by: Optional[GroupBy],
        modifications: List[str],
        conditions,
        alias_name: str,
    ) -> Tuple[str, List[str], List[str], str]:
        if not isinstance(conditions, list):
            conditions = [conditions]

        metric_name = self._replace_invalid_metric_characters(metric_name)
        expressions = []
        over_times = []
        for condition in conditions:
            expr = ""
            over_time_aggregations = set()
            over_time = group_by.over_time if group_by else None
            aggregation = aggregations[-1] if aggregations else ""
            if group_by:
                if not over_time:
                    if "count" in aggregations:
                        over_time = "$__range"
                else:
                    for aggregation in aggregations:
                        if aggregation in OVER_TIME_AGGREGATIONS:
                            if "rate" in aggregations:
                                over_time_aggregations.add("rate")
                            elif "increase" in aggregations:
                                over_time_aggregations.add("increase")
                            else:
                                over_time_aggregations.add(f"{aggregation}_over_time")
            if divide_by_self:
                if not condition:
                    condition = "!= 0"
                # conditions are of 'op value' format. We convert them to
                # op bool value
                condition = condition.replace(" ", " bool ", 1)
            if condition == "> 0" and aggregation == "sum":
                condition = ""
                # Extra zero results from m3 are fine
            if aggregation:
                if aggregation in {"rate", "increase"} and not over_time_aggregations:
                    over_time_aggregations.add(aggregation)
                    # Default to 'avg' aggregation + $__rate_interval in this case
                    aggregation = self.get_metric_aggregation(metric_name)
                    if not over_time:
                        over_time = "$__rate_interval"
                if group_by and (group_by.group_by or over_time_aggregations):
                    if (
                        not condition
                    ):  # count works better than sum if count_over_time is omitted
                        old_agg = aggregation  # Dawid
                        aggregation = AGGREGATION_TOPLEVEL_MAP.get(
                            aggregation, aggregation
                        )
                        # DG change, force count_over_time that somehow is not detected
                        if old_agg == "count" and aggregation == "sum":
                            over_time_aggregations = {"count_over_time"}

                    # Dawid change - use plain group() on top of expressions, do not discard
                    if (
                        not over_time
                    ):  # discard aggregation from group by when over time function is used
                        expr += "{}{}".format(
                            aggregation,
                            self._replace_invalid_metric_characters(group_by.group_by),
                        )
                    else:
                        expr += f"{aggregation}{self._replace_invalid_metric_characters(group_by.group_by)}"
                else:
                    expr += aggregation
            if condition and set(aggregations).difference({"avg", "max", "min"}):
                over_time_aggregations = set()
                # We can't do over time aggregation in this case (as it is
                # done before filtering); hopefully result is still ok and
                # covered by e.g. over_time.
                #
                # e.g. count, and sum break badly if filtering order is wrong
            expr += "("
            inner_expr = ""
            if over_time_aggregations:
                inner_expr += "{}(".format(" ".join(over_time_aggregations))
            inner_expr += metric_name
            if labels:
                inner_expr += f"{{{labels}}}"
            if over_time:
                scrape_interval = self.scrape_interval
                for (
                    metric_prefix,
                    scrape_interval_override,
                ) in NONDEFAULT_METRIC_PREFIX_SCRAPE_INTERVAL_SECONDS.items():
                    if metric_name.startswith(metric_prefix):
                        scrape_interval = scrape_interval_override
                if "rate" in aggregations or "increase" in aggregations:
                    # With default scrape interval, we just assume using __rate_interval is enough
                    if (
                        over_time in {"$__rate_interval", "$__interval"}
                        and scrape_interval == self.scrape_interval
                    ):
                        over_time = "$__rate_interval"
                    else:
                        over_times.append(_seconds_to_duration(scrape_interval * 4))
                else:
                    over_times.append(_seconds_to_duration(scrape_interval))
                if over_time_aggregations:
                    # replace time frame i.e 5m, 1h etc with $__interval
                    interval_replace = re.sub(
                        TIME_INTERVAL_REGEX, "$__interval", over_time
                    )
                    inner_expr += f"[{interval_replace}]"
                over_times.append(over_time)
            if over_time_aggregations:
                inner_expr += f"){condition}"
            else:
                inner_expr += "{conditions}){modifications}".format(
                    modifications=" ".join(modifications), conditions=condition
                )
            expr += inner_expr
            if over_time_aggregations:
                expr += ") {modifications}".format(
                    modifications=" ".join(modifications)
                )
            expressions.append(expr.replace("'", "'"))
        fills = group_by.fills if group_by else []
        return " or ".join(expressions), over_times, fills, alias_name

    def get_metric_name(self, query: str) -> Tuple[str, str]:
        # udpated_query = query.replace("-", "_")
        part1 = re.search(r'from "(\S+)" where', query, re.IGNORECASE)
        if part1 is None:
            raise ValueError(f"Unable to find metric name in {query}")
        # Added |. after word selection for specific dashboard with a . in the metric name
        part2 = re.search(r'(\("|\(|\")([\w.]+)("\)|\)|")', query, re.IGNORECASE)
        if part2 is None:
            raise ValueError(f"Unable to find (single) metric key in {query}")
        return part1.group(1), (
            part2.group(1) if part2.group(1) not in {'"', "(", '("'} else part2.group(2)
        )

    def get_aggregations(self, query: str) -> List[str]:
        part1 = re.search(r"select (.*) from", query, re.IGNORECASE)
        if part1 is None:
            raise ValueError(f"Unable to find aggregations in {query}")
        part2 = part1.group(1).split("(")
        part3 = [AGGREGATION_MAP[item] for item in part2 if item in AGGREGATION_MAP]
        return part3

    def get_labels(self, query: str, *, field_name: str) -> Tuple[str, str]:
        """Converts any equals, not equals, like and not like InfluxQL where conditions into corresponding Prometheus conditions.

        E.g. "abc" = def AND foo =~ /bar$/ => abc='def',foo=~'.*bar'
        """
        labels = set()

        # Bit hacky regexp handling trick:
        #
        # Replace (within parentheses) set of ORs with equalities with
        # single regexp
        single_eq_re = r""""(\S+)"\s*=\s*'(\S+)'"""

        def _replace(m):
            original_string = m.group(1)
            results = re.findall(single_eq_re, m.group(1))
            keys = set(r[0] for r in results)
            if len(keys) == 1:
                # only r[1]s differ -> we can create regexp to match them
                key = next(iter(keys))
                values = "|".join(r[1] for r in results)
                labels.add(f'{key}=~"{values}"')
                return ""
            return original_string

        query = re.sub(
            f"(\\({single_eq_re}\\s+(OR\\s+{single_eq_re})*\\))",
            _replace,
            query,
            re.IGNORECASE,
        )

        for operator in LABEL_COMPARISON_OPERATORS:
            if operator in {"!~", "=~"}:
                # Regex matches are of the form "field" =~ /regex/. regex could contain escaped forward
                # slash so only end when finding / that isn't preceded by uneven number of backslashes.
                # Actual content matching is non-greedy as there may be forward slashes later in content.
                # Field name may or may not be double quoted. If it isn't, it should be simple alphanumeric
                # string.
                regex = r'(\w+?|"\S+?")\s*{}\s*/(.*?)(?<!\\)(?:\\{{2}})*/'.format(
                    operator
                )
                for key, value in re.findall(regex, query):
                    key = key.lstrip('"').rstrip('"').replace("-", "_")
                    if key == field_name:
                        continue
                    # InfluxQL regexes are search-like (match anywhere) while Prometheus does exact
                    # matches. Need to convert patterns accordingly
                    if value.startswith("^"):
                        value = value[1:]
                    elif not value.startswith(".*"):
                        value = f".*{value}"
                    if value.endswith("$"):
                        value = value[:-1]
                    elif not value.endswith(".*"):
                        value = f"{value}.*"
                    # Forward slashes in the query have been escaped, strip the escapes for Prometheus
                    value = value.replace("\\/", "/")
                    # Regex escapes need to be escaped themself
                    value = value.replace("\\", "\\\\")
                    # Need to escape single quotes
                    value = value.replace("'", "\\'")
                    labels.add(f'{key}{operator}"{value}"')
            else:
                # Non regex matches are of the form "field" = foobar or "field" = 'foo-bar'. Look for
                # both forms separately. Field name may or may not be double quoted. If it isn't, it
                # should be simple alphanumeric string.
                regex = r'(\w+?|"\S+?")\s*{}\s*\'(.*?)(?<!\\)(?:\\{{2}})*\''.format(
                    operator
                )

                def _escape_backslashes(s):
                    # There's stuff like "aiven\.prune" floating
                    # around in exact matches, and m3 doesn't like
                    # escapes in the normal strings that it does not
                    # support (it only supports \n\r or something I
                    # suppose)
                    return s.replace("\\", "\\\\")

                for key, value in re.findall(regex, query):
                    key = key.lstrip('"').rstrip('"')
                    if key == field_name:
                        continue
                    value = _escape_backslashes(value)
                    labels.add(f'{key}{operator}"{value}"')
                # Non-quoted value variant is terminated either by whitespace or closing parenthesis
                regex = r'(\w+?|"\S+?")\s*{}\s*([^\s\'~].*?)(?:\)|\s)'.format(operator)
                for key, value in re.findall(regex, query):
                    key = key.lstrip('"').rstrip('"')
                    if key == field_name:
                        continue
                    value = _escape_backslashes(value)
                    labels.add(f'{key}{operator}"{value}"')

        return query, ",".join(sorted(labels))

    def get_conditions(self, query: str, *, field_name: str) -> Union[str, List[str]]:
        conditions = []
        for operator in LABEL_CONDITIONS_OPERATORS:
            for key, value in re.findall(
                r'(\w+?|"\S+?")\s*{}\s*(-?\w+(?:\.?\d+)?)'.format(operator), query
            ):
                key = key.strip('"')
                # Prometheus queries don't support filtering by values that are not actually selected
                if key != field_name:
                    if operator == "=":
                        continue
                    raise ValueError(
                        f"Query {query!r} has condition that does not match select field, cannot convert"
                    )
                if operator == "=":
                    operator = "=="
                conditions.append(f"{operator} {value}")
        # To make this work reliably we should actually parse the query but in our case there are only couple
        # of queries that use OR and all of those are simple queries with nothing but the OR. For Prometheus
        # queries OR needs to generate multiple different queries, otherwise the conditions are treated as AND
        if " OR " in query and conditions:
            return conditions
        else:
            return " ".join(conditions)

    def does_divide_by_self(self, query: str) -> bool:
        """Returns True if the query contains select like 'SELECT max("foo") / max("foo") FROM somwhere'."""
        return bool(
            re.search(r"select\s+(\S+?)\s*/\s*\1\s+from ", query, re.IGNORECASE)
        )

    def get_modifications(self, query: str) -> List[str]:
        modifications = []
        # Basic arithmetic operations where the other side is a number
        part1 = re.search(
            r'[")](\s*[*/+\-]\s*-?[\d]+?(?:\.\d+?)?) from', query, re.IGNORECASE
        )
        if part1:
            modifications.append(part1.group(1))
        return modifications

    def get_group_by(self, query: str) -> Optional[GroupBy]:
        self.group_by_labels = None
        fills = []
        group_by = []
        time_val = ""
        groups = query.lower().split("group by")
        if len(groups) < 2:
            return None
        groups = groups[1].split(" ")
        for outer_group in groups:
            inner_groups = outer_group.split(",")
            for group in inner_groups:
                if not group:
                    continue
                m = re.match(r"(?i)fill\((.*)\)", group)
                if m is not None:
                    fills.append(m.group(1))
                    continue
                if "time" in group:
                    m = re.search("\\((.*)\\)", group)
                    if m is None:
                        raise ValueError(f"Unparseable group by statement: {group}")
                    time_val = (
                        m.group(1)
                        .replace("$_interval", "$__interval")
                        .replace("auto", "$__interval")
                        .replace("$interval", "$__interval")
                    )
                    continue
                group_by.append(group.replace(",", "").replace('"', ""))
        self.group_by_labels = group_by
        group_by_str = " by ({})".format(",".join(group_by)) if group_by else ""
        return GroupBy(group_by=group_by_str, over_time=time_val, fills=fills)

    def get_alias_from_query(self, query: str) -> str:
        # udpated_query = query.replace("-", "_")
        m = re.search(
            r'(\("|\(|\")(?P<field>[\w.]+)("\)|\)|")( AS [\'"]?(?P<alias>\w+)[\'"]?)?',
            query,
            re.IGNORECASE,
        )
        if m is None:
            raise ValueError(f"Unable to find (single) metric key in {query}")
        return m.groupdict().get("alias", "")

    def convert_expression(self, query: str) -> Tuple[str, List[str], List[str], str]:
        series_name, field_name = self.get_metric_name(query)
        # DG - split by "." and remove the database given this way - not needed in our case
        series_name = series_name.split(".")[-1].replace('"', "")
        alias_name = self.get_alias_from_query(query)
        field_name = field_name.replace(
            ".", "_"
        )  # Specific use case for metric names with . as a seperator
        metric_name = "{}_{}".format(series_name, field_name)
        aggregations = self.get_aggregations(query)
        if aggregations == ["avg"]:
            default_aggregation = self.get_metric_aggregation(metric_name)
            aggregations = [default_aggregation]
        query, labels = self.get_labels(query, field_name=field_name)
        modifications = self.get_modifications(query)
        group_by = self.get_group_by(query)
        if not aggregations and group_by and group_by.group_by:
            # We need to have *some* aggregation to group by
            aggregations = [self.get_metric_aggregation(metric_name)]
            if aggregations == ["avg"]:
                self._error_manager.add_error(
                    f"Using default aggregation of avg for {query}", error_level="WARN"
                )

        conditions = self.get_conditions(query, field_name=field_name)
        divide_by_self = self.does_divide_by_self(query)
        if divide_by_self and (conditions or modifications):
            raise ValueError(
                f"Unsupported query {query}, divide by self combined with conditions or modifications"
            )
        return self.format_expression(
            divide_by_self=divide_by_self,
            metric_name=metric_name,
            aggregations=aggregations,
            labels=labels,
            group_by=group_by,
            modifications=modifications,
            conditions=conditions,
            alias_name=alias_name,
        )

    def convert_subquery(self, query: str) -> Tuple[str, List[str], List[str]]:
        # In-InfluxDB downsampling case:
        #
        # SUM .. FROM ( subquery GROUP BY X, Z ) GROUP BY X
        # =~
        # PromQL:
        #
        # sum by (X)(converted subquery without outer aggregation)
        m = re.match(
            r"""
        \s*SELECT\s+SUM\("(?P<outer_key>[a-z]+)"\)\s*
        FROM\s*\(
          (?P<inner1>\s*SELECT\s*[^\s]+\s*)
          (AS\s\S+\s+)?
          (?P<inner2>.*)\s+
          GROUP\s+BY.*
        \)\s*
        (?P<outer_group>GROUP\sBY\s.*)
        """,
            query,
            re.VERBOSE | re.IGNORECASE,
        )
        if m is not None:
            d = m.groupdict()
            inner1 = d["inner1"]
            inner2 = d["inner2"]
            outer_group = d["outer_group"]
            subquery = f"{inner1} {inner2} {outer_group}"
            expr, over_times, fills = self.convert_expression(subquery)
            # This could be neater if we actually passed the desired outer
            # function to convert/format expression.
            if over_times:
                m = re.match(r"\S+ by (\((.*))", expr)
                if m is not None:
                    rest = m.group(1).strip()
                    expr = f"sum by {rest}"
                    return expr, over_times, fills
        raise ValueError(f"Unsupported subquery in {query!r}")

    def convert_special_or_expression(self, query):
        # Handling for 'special' expressions that we hardcode.
        #
        # We should have real parser but oh well.
        m = re.match(r"\s*SELECT (.*) FROM (.*)", query, re.IGNORECASE)
        if m is None:
            raise ValueError(f"Unable to parse initial SELECT part: {query}")
        return_part, from_tail = m.groups()
        return_part = re.sub(r"\s+", " ", return_part).strip()
        if return_part == 'mean("num_fds") / mean("rlimit_num_fds_soft") * 100':
            r = self.convert_expression(f'SELECT mean("num_fds") FROM {from_tail}')
            exp_re = r"avg_over_time\(procstat_num_fds({[^}]+}\[[^\]]+\])\)"
            assert re.search(exp_re, r[0]) is not None, f"Unexpected input: {r[0]}"

            def _sub(m):
                s = m.group(1)
                return f"avg_over_time(procstat_num_fds{s}) / avg_over_time(procstat_rlimit_num_fds_soft{s}) * 100"

            new_q = re.sub(exp_re, _sub, r[0]).strip()
            # If rlimit_num_fds_soft doesn't happen to come with
            # num_fds (it happens sometimes, prom or m3 weirdness?), result
            # is Inf. So we filter it explicitly in the end,
            # pretending there is no datapoint at all even if num_fds
            # had one.
            return (f"{new_q} != Inf", r[1], r[2])

        m = re.match(r"(\d+)\s?([-+*/])\s?(.*)$", return_part)
        if m is not None:
            # Redis panel specialty; this math doesn't really work as is
            constant, op, expr = m.groups()
            r = self.convert_expression(f"SELECT {expr} FROM {from_tail}")
            new_q = r[0].strip()
            return (f"{constant} {op} {new_q}", r[1], r[2])

        return self.convert_expression(query)

    def convert_query(
        self, query: str, target: dict, legend: str
    ) -> Tuple[dict, List[str], List[str]]:
        if "<>" in query:
            raise ValueError(
                f"Unexpected <> found from query {query!r}, use != instead"
            )
        if "(*)" in query:
            raise ValueError("Unsupported (*) query in {query!r}")
        if "from (" in query.lower():
            expr, over_times, fills, alias_name = self.convert_subquery(query)
        else:
            expr, over_times, fills, alias_name = self.convert_expression(query)
        fmt = target["resultFormat"]
        if "$col" in legend:
            if alias_name:
                legend = legend.replace("$col", alias_name)
            else:
                self._error_manager.add_error(
                    "Alias not detected, not replacing $col", error_level="WARN"
                )
        new_target = {
            "expr": expr,
            "format": fmt,
            "instant": fmt == "table",
            "intervalFactor": 1,
            "refId": target["refId"],
            "legendFormat": legend,
        }

        if "hide" in target:
            new_target["hide"] = target["hide"]
        return new_target, over_times, fills

    def convert_to_query(self, target: dict, legend: str) -> str:
        query = "SELECT "
        value = ""
        select_what = ""
        alias = ""
        modifications: List[str] = []
        if "not-influx-target" in target and target["not-influx-target"]:
            return "not-influx-target", [], []
        if target.get("select") is None:
            raise ValueError(
                f"Dashboard {self._current_dashboard} is invalid, missing select field in target"
            )

        for select in target["select"]:
            for item in sorted(select, key=lambda x: x["type"].lower()):
                item_type = item["type"]
                if item_type == "field":
                    value = item["params"][0].replace("::tag", "")
                elif item_type == "math":
                    modifications.extend(item["params"])
                elif item_type in AGGREGATION_MAP:
                    param_str = ""
                    if item["params"]:
                        param_str = ", {}".format(
                            str(item["params"][0]).replace("::tag", "")
                        )
                    if select_what:
                        select_what = f"{item_type}({select_what}{param_str})"
                    elif item_type not in ("max", "last"):
                        select_what = (
                            f'{item_type}("{value}"{param_str}){{alias}}'.format(
                                alias=f' AS "{alias}"' if alias else ""
                            )
                        )
                elif item_type == "alias":
                    # This is only used in the Maps dashboard. This is actually relevant but the map itself is
                    # of questionable value so just ignore the issue for now
                    alias = item["params"][0]
                elif item_type == "distinct":
                    self._error_manager.add_error(
                        f"Dropping unsupported item type: distinct"
                    )
                else:
                    raise ValueError(f"Unknown item type {item_type!r} in {target!r}")

        select_what = f'"{value}"' if select_what == "" else select_what

        where_items = ["$timeFilter"]
        if "tags" in target:
            new_regex = None
            if any(tag.get("condition") == "OR" for tag in target["tags"]):
                # Prometheus queries don't directly support label selection using OR. We can convert OR into
                # regular expression but only if all tags have the same key
                keys = [tag["key"] for tag in target["tags"]]
                if len(set(keys)) != 1:
                    # Simplify error - DG
                    # raise ValueError(
                    #     f"Unsupported OR for tags with different key: {target!r}"
                    # )
                    raise ValueError(f"Unsupported OR for tags with different key")
                # We could support operators other than = but that would make the regex generation a bit difficult
                # Doing it anyway - DG
                regex_parsing = False
                if not all(tag.get("operator") == "=" for tag in target["tags"]):
                    if not all(tag.get("operator") == "=~" for tag in target["tags"]):
                        # raise ValueError(f"Unsupported operator in OR tag: {target!r}")
                        # Simplify error - DG
                        raise ValueError(f"Unsupported operator in OR tag")
                    else:
                        regex_parsing = True

                cleaned_up_regexes = []
                start_boundaries_count = 0
                end_boundaries_count = 0
                if regex_parsing:
                    for tag in target["tags"]:
                        cleaned_up_regex = (
                            tag["value"].replace("::tag", "").replace("/", "").strip()
                        )
                        if cleaned_up_regex.startswith("^"):
                            cleaned_up_regex = cleaned_up_regex[1:]
                            start_boundaries_count += 1
                        else:
                            cleaned_up_regex = f".*{cleaned_up_regex}"

                        if cleaned_up_regex.endswith("$"):
                            cleaned_up_regex = cleaned_up_regex[:-1]
                            end_boundaries_count += 1
                        else:
                            cleaned_up_regex = f"{cleaned_up_regex}.*"
                        cleaned_up_regexes.append(cleaned_up_regex)

                    if not (
                        end_boundaries_count == 0
                        or end_boundaries_count == len(cleaned_up_regexes)
                    ):
                        self._logger.warning(
                            "End boundaries are different - resulting regex can be different than intended !!!! Skipping end boundary"
                        )
                        self._error_manager.add_error(
                            "End boundaries are different - resulting regex can be different than intended !!!! Skipping end boundary"
                        )
                        end_boundaries_count = 0
                    if not (
                        start_boundaries_count == 0
                        or start_boundaries_count == len(cleaned_up_regexes)
                    ):
                        self._error_manager.add_error(
                            "Start boundaries are different - resulting regex can be different than intended !!!! Skipping start boundary"
                        )
                        start_boundaries_count = 0

                    new_regex = '"{key}"=~/^({regexes})$/'.format(
                        key=keys[0],
                        regexes="|".join(cleaned_up_regexes),
                    )

                if new_regex:
                    where_items.append(new_regex)
                else:
                    values = [
                        tag["value"].replace("::tag", "") for tag in target["tags"]
                    ]
                    where_items.append(
                        '"{key}"=~/^{value}$/'.format(
                            key=keys[0], value="|".join(values)
                        )
                    )
            else:
                for tag in target["tags"]:
                    operator = tag["operator"]
                    # Our query parsing doesn't handle <> correctly, use != instead
                    if operator == "<>":
                        operator = "!="
                    where_items.append(
                        '"{key}"{operator}{value}'.format(
                            key=tag["key"].replace("::tag", ""),
                            operator=operator,
                            value=tag["value"],
                        )
                    )
        where = "({})".format(" AND ".join(where_items))

        group_by_items = []
        for group in target["groupBy"]:
            if group["type"] == "time":
                group_by_items.append(
                    "{}({})".format(group["type"], group["params"][0])
                )
            elif group["type"] == "tag":
                group_by_items.append(
                    '"{}"'.format(group["params"][0].replace("::tag", ""))
                )
            elif group["type"] == "fill":
                # The "Null value: connected" visualization option should be used instead of using fill query
                continue
            else:
                group_type = group["type"]
                raise ValueError(f"Unknown group type {group_type} in {target}")

        group_by = (
            " GROUP BY {}".format(",".join(group_by_items)) if group_by_items else ""
        )
        if not target.get("measurement"):
            raise ValueError(
                f"Missing measurement field for target, in dashboard: {self._current_dashboard}"
            )

        query += '{select_what}{modifications} FROM "{measurement}" WHERE {where}{group_by}'.format(
            select_what=select_what,
            modifications=" ".join(modifications),
            measurement=target["measurement"],
            where=where,
            group_by=group_by,
        )
        return query

    def convert_series_overrides(self, overrides) -> List[dict]:
        new_overrides = []
        for override in overrides:
            override = dict(override)
            # We may have converted aliases used in legend from percentile to mean. If there's visualization
            # override for the same series we need to update that as well.
            if override.get("alias"):
                override["alias"] = re.sub(
                    r"\d+\w+\spercentile", "Mean", override["alias"]
                )
            new_overrides.append(override)
        return new_overrides

    def get_legend_format(self, target: dict) -> str:
        legend = re.sub(
            r"(\[\[|\$)tag_([a-zA-Z_]+)(\]\]|)", r"{{ \2 }}", target.get("alias", "")
        )
        # We don't support percentile, if legend says something is percentile convert that to Mean instead
        legend = re.sub(r"\d+\w+\spercentile", "Mean", legend)
        if legend == "" and self.group_by_labels:
            legend_list = ["{{" + label + "}}" for label in self.group_by_labels]
            legend = " ".join(legend_list)

        return legend

    def convert_targets(
        self, targets: List[dict], old_panel_datasource: dict
    ) -> Tuple[List[dict], List[str], List[str]]:
        new_targets = []
        r_over_times = []
        r_fills = []

        for target in targets:
            if self._influx_detector.is_target_influx(
                target, self._context_templating, old_panel_datasource
            ):
                legend = self.get_legend_format(target)
                if legend:
                    legend = self._replace_invalid_metric_characters(
                        legend
                    )  # replace to promql standard
                if target.get("rawQuery"):  # Some panels use raw query instead
                    query = target["query"].replace("\n", " ")
                    query = re.sub("  +", " ", query)
                    new_target, over_times, fills = self.convert_query(
                        query, target, legend
                    )
                else:
                    query = self.convert_to_query(target, legend)
                    new_target, over_times, fills = self.convert_query(
                        query, target, legend
                    )
                if new_target == "not-influx-target":
                    new_targets.append(target)
                    fills = []
                    over_times = []
                else:
                    new_targets.append(new_target)
                r_fills.extend(fills)
                r_over_times.extend(over_times)
            else:

                self._logger.debug(f"Skipping {target} - not Influx")
                self._error_manager.add_error(
                    "Skipping target - not Influx or is Flux",
                    notes=(
                        f'target={target["refId"]}'
                        if "refId" in target
                        else "target unknown"
                    ),
                    error_level="DEBUG",
                )
                new_target = deepcopy(target)
                new_target["not-influx-target"] = True
                new_targets.append(new_target)
                r_fills.extend([])
                r_over_times.extend([])
        return new_targets, r_over_times, r_fills

    def convert_panel(self, panel: dict) -> None:
        self._error_manager.context.panel = GrafanaPanel(
            id=panel["id"],
            title=panel["title"] if "title" in panel else panel["id"],
        )
        if panel["type"] == "row":
            # Collapsed panels are not listed in toplevel panels, but
            # instead under 'panels' list of the 'row'
            self.convert_panels(panel.get("panels", []))
            return

        old_panel_datasource = None
        if "datasource" in panel and panel["datasource"] is not None:
            old_panel_datasource = panel["datasource"]
        else:
            old_panel_datasource = {"type": "influxdb"}  # Default

        if self.replacement_datasource:
            if "datasource" in panel:
                panel["datasource"] = self.replacement_datasource
        elif self.datasource_map:
            datasource = panel.get("datasource")
            if datasource and (
                isinstance(datasource, dict) or datasource not in self.datasource_map
            ):
                return
            panel["datasource"] = self.datasource_map[panel.get("datasource")]
        try:
            if "targets" in panel:
                targets, over_times_list, fills_list = self.convert_targets(
                    panel["targets"], old_panel_datasource
                )

                self.add_to_metric_and_object_list(
                    panel["targets"], targets, old_panel_datasource
                )
                panel["targets"] = targets
                over_times = set(over_times_list)
                # Assume $__interval and $__rate_interval are covered by scraping interval
                over_times.discard(
                    "$interval",
                )
                over_times.discard("$interval")
                over_times.discard("$interval")
                over_times.discard("$__interval")
                over_times.discard("$__rate_interval")
                # DG not parse funny intervals
                for item in over_times.copy():
                    if (
                        item.find("$__interval") != -1
                        or item.find("$Interval") != -1
                        or item.find("$unite_interval") != -1
                        or item.find("$__rate_interval") != -1
                    ):
                        over_times.discard(item)
                        self._error_manager.add_error(
                            "Dropping unsupported interval", notes=item
                        )
                if (
                    len(over_times) > 0
                    and not panel.get("interval", None)
                    and "$__range" not in over_times
                ):
                    over_time_seconds = max(_duration_to_seconds(x) for x in over_times)
                    if over_time_seconds != SCRAPE_INTERVAL_SECONDS:
                        over_time = _seconds_to_duration(over_time_seconds)
                        panel["interval"] = over_time
                fills = set(fills_list)
                fills.discard("null")  # implicit
                if "0" in fills:
                    fills.discard("0")
                    panel["nullPointMode"] = "null as zero"
                if fills:
                    self._logger.warning("Unsupported fills: %r", fills)
                    self._error_manager.add_error("Unsupported fills", notes=fills)
            if "alert" in panel:
                self.convert_alerts(panel["alert"], panel["title"])
        except ValueError as ex:
            panel.pop("targets", None)
            panel.pop("alert", None)
            self._error_manager.add_error(
                f"Unable to convert - {ex}", error_level="ERROR", notes=f"{ex}"
            )
        if panel.get("seriesOverrides"):
            panel["seriesOverrides"] = self.convert_series_overrides(
                panel["seriesOverrides"]
            )

    def convert_panels(self, panels: List[dict]) -> None:
        for panel in panels:
            self.convert_panel(panel)

    def convert_dashboard(self, dashboard: dict, meta: dict) -> dict:
        self._error_manager.context.meta = meta
        self._logger.info(f'Started dashboard conversion for: {dashboard.get("title")}')
        self._context_templating = dashboard["templating"]
        self._error_manager.context.dashboard = GrafanaDashboard(
            uid=dashboard["uid"],
            title=dashboard["title"],
            folder=meta["folderTitle"],
            updater=meta["updatedBy"],
        )
        self._error_manager.context.folder = meta["folderTitle"]
        self._current_dashboard = dashboard.get("title")
        # Reverted order - first Panels, then Templating - DG
        self.convert_panels(dashboard.get("panels", []))
        for row in dashboard.get("rows", []):
            # TBD does this even exist? modern Grafana seems to have just toplevel list of "panels"
            self.convert_panels(row["panels"])
        templating = dashboard.get("templating")
        try:
            if templating:
                self.convert_templating(dashboard["templating"])
        except ValueError as ex:
            raise ConvertError(ex) from ex

        for r in self._get_rep_metric:
            if r[0]:
                self._update_label_values_metric()

        self._logger.info(
            f'Finished dashboard conversion for: {dashboard.get("title")}'
        )
        return dashboard

    def _update_label_values_metric(self):
        for r in self._get_rep_metric:
            if r[0] is not True:
                continue
            dashboard_metric = r[1]
            label_values = r[2]
            for metric in self.metric_to_objects.keys():
                splitted_metric = metric.rsplit("_", maxsplit=1)
                if len(splitted_metric) < 2:
                    continue
                if splitted_metric[0] == dashboard_metric or metric == dashboard_metric:
                    label_values["query"] = label_values["query"].replace(
                        dashboard_metric, metric
                    )
                    try:
                        self.metric_to_objects[metric][self._current_dashboard].append(
                            label_values
                        )
                    except:
                        self._error_manager.add_error(
                            "Invalid dashboard on templating replacement split",
                            error_level="ERROR",
                            notes=(
                                self._current_dashboard
                                if self._current_dashboard
                                else "None"
                            ),
                        )
                    break
        self._get_rep_metric = []
