"""解析发布门槛：按来源格式试解析样本，用质量规则判定候选规则能否生效。

核心概念
--------
* 样本（``_parser_samples``）：每个来源/输入格式保留带期望字段的原始日志样本，
  是字段映射质量的唯一校验依据。
* 候选（``_parser_candidates``）：一次规则变更，声明要纳入哪些输入格式、
  每个格式使用什么字段映射，以及标准化后的必填字段。
* 试解析（``_parser_trials``）：按格式独立解析样本并计算缺字段率/错列率，
  按质量校验规则的阈值逐格式判定。样本结果会保留，阈值变化时可重算。
* 发布清单（``_parser_manifests``）：候选发布后的资格记录。只有试解析合格、
  且确实发布成功的格式才在清单里标记为可生产使用；超限格式被明确阻断。
* 发布日志（``_publish_journal``）：试解析、暂存、中断、恢复、提交、重算的
  审计轨迹，也是中断恢复的依据。

并发约定
--------
发布分“暂存规则 → 原子提交清单”两步。中断时清单不生效、暂存规则仍是草稿，
生产解析继续读取上一份生效清单；生产批次开始时一次性快照当前清单并固定
规则版本，批次进行中即使发布完成也不会换错映射。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.store import store

DEFAULT_MISSING_RATE = 0.05
DEFAULT_MISALIGNED_RATE = 0.02

MISSING_METRIC = "缺字段率"
MISALIGNED_METRIC = "错列率"
MISSING_ALIASES = {MISSING_METRIC, "字段完整性", "缺失字段率"}
MISALIGNED_ALIASES = {MISALIGNED_METRIC, "列对齐", "错列"}

WILDCARDS = {"", "*", "全部", "all", "ALL"}


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _required_fields(candidate: dict[str, Any]) -> list[str]:
    raw = candidate.get("必填字段") or ""
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _rule_fields(rule: dict[str, Any]) -> list[str]:
    raw = rule.get("输出字段列表") or ""
    return [item.strip() for item in str(raw).split(",") if item.strip()]


# ---------------------------------------------------------------------------
# 解析引擎
# ---------------------------------------------------------------------------

def parse_body(body: str, rule: dict[str, Any]) -> dict[str, str]:
    """按规则解析一行原始日志。

    支持的解析引擎写法：
    * ``delimited:<分隔符>``：按分隔符切列，列顺序即输出字段顺序；
    * ``json`` / ``jsonl``：按字段名从 JSON 对象取值；
    * ``kv`` / ``kv::``：按 ``k=v`` 形式取值；
    * 其他（历史占位规则）：保持原有 ``|`` 切分兜底行为。

    规则可以用 ``字段映射`` 把标准化字段名映射到源字段名，
    例如 ``{"timestamp": "ts", "message": "msg"}``。
    """
    text = "" if body is None else str(body)
    engine = str(rule.get("解析引擎") or "").strip()
    fields = _rule_fields(rule)
    mapping = rule.get("字段映射") or {}
    if isinstance(mapping, str):
        parsed_mapping = _json_or_none(mapping)
        mapping = parsed_mapping if isinstance(parsed_mapping, dict) else {}
    lower = engine.lower()

    if lower.startswith("delimited"):
        delimiter = engine.split(":", 1)[1] if ":" in engine else "|"
        delimiter = {"\\t": "\t", "tab": "\t"}.get(delimiter, delimiter)
        parts = text.split(delimiter) if delimiter else [text]
        return {
            field: (parts[index].strip() if index < len(parts) else "")
            for index, field in enumerate(fields)
        }

    if lower in {"json", "jsonl"}:
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return {field: "" for field in fields}
        if not isinstance(payload, dict):
            return {field: "" for field in fields}
        return {
            field: _as_text(payload.get(mapping.get(field, field), ""))
            for field in fields
        }

    if lower == "kv" or lower.startswith("kv:"):
        separator = engine.split(":", 1)[1] if ":" in engine else "="
        parsed: dict[str, str] = {}
        for chunk in text.replace(",", " ").split():
            if separator not in chunk:
                continue
            key, value = chunk.split(separator, 1)
            parsed[key.strip()] = value.strip()
        return {
            field: parsed.get(mapping.get(field, field), "")
            for field in fields
        }

    # 历史规则：没有可识别引擎时沿用原来的行为，保证旧调用不受影响。
    parts = text.split("|") if "|" in text else [text]
    if not fields:
        fields = ["time", "level", "message"]
    return {
        field: (parts[index].strip() if index < len(parts) else "")
        for index, field in enumerate(fields)
    }


# ---------------------------------------------------------------------------
# 阈值解析
# ---------------------------------------------------------------------------

def _float_or_none(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _json_or_none(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError):
        return None


def _scoped_value(node: Any, source: str, fmt: str) -> tuple[float | None, int]:
    """从阈值参数里找出适配当前来源/格式的值，返回 (阈值, 具体度)。"""
    value = _float_or_none(node)
    if value is not None:
        return value, 0
    if not isinstance(node, dict):
        return None, -1

    # {"阈值": 0.05, "输入格式": "app-text-v1"} 形式
    if "阈值" in node or "value" in node:
        scoped_fmt = str(node.get("输入格式") or node.get("format") or "")
        scoped_source = str(node.get("适用数据源") or node.get("source") or "")
        if scoped_fmt not in WILDCARDS and scoped_fmt != fmt:
            return None, -1
        if scoped_source not in WILDCARDS and scoped_source != source:
            return None, -1
        value = _float_or_none(node.get("阈值", node.get("value")))
        if value is None:
            return None, -1
        # 具体度：全局=0，只限定格式=1，只限定来源=2，来源+格式=3。
        specificity = 1 + 2 * int(scoped_source not in WILDCARDS) + int(scoped_fmt not in WILDCARDS)
        return value, specificity

    best_value: float | None = None
    best_specificity = -1
    # 直接的格式映射：{"app-text-v1": 0.02, "*": 0.05}
    for key in (fmt, "*"):
        if key in node:
            value, specificity = _scoped_value(node[key], source, fmt)
            if value is not None and specificity + (1 if key == fmt else 0) > best_specificity:
                best_value, best_specificity = value, specificity + (1 if key == fmt else 0)
    # 来源 → 格式的嵌套映射（最具体），例如 {"app-gateway": {"app-text-v1": 0.01}}
    for source_key in (source, "*"):
        if source_key in node and isinstance(node[source_key], dict):
            value, specificity = _scoped_value(node[source_key], source, fmt)
            if value is not None and specificity + (2 if source_key == source else 0) > best_specificity:
                best_value, best_specificity = value, specificity + (2 if source_key == source else 0)
    return best_value, best_specificity


def _source_applies(rule: dict[str, Any], source: str) -> bool:
    scope = str(rule.get("适用数据源") or "").strip()
    return scope in WILDCARDS or scope == source


def resolve_threshold(metric: str, source: str, fmt: str) -> dict[str, Any]:
    """找出当前生效的阈值规则；按 来源+格式 → 来源 → 格式 → 全局 取最具体。"""
    aliases = MISSING_ALIASES if metric == MISSING_METRIC else MISALIGNED_ALIASES
    default = DEFAULT_MISSING_RATE if metric == MISSING_METRIC else DEFAULT_MISALIGNED_RATE
    best: dict[str, Any] | None = None
    best_specificity = -1
    for rule in store.rows("quality_check"):
        if rule.get("status") != "已生效":
            continue
        if str(rule.get("校验类型") or "").strip() not in aliases:
            continue
        if not _source_applies(rule, source):
            continue
        raw = rule.get("阈值参数")
        value, specificity = _scoped_value(raw, source, fmt)
        if value is None:
            parsed = _json_or_none(raw)
            if parsed is not None:
                value, specificity = _scoped_value(parsed, source, fmt)
        if value is None:
            continue
        if specificity > best_specificity:
            best = {
                "阈值": value,
                "规则ID": rule.get("id"),
                "规则名称": rule.get("规则名称"),
                "校验类型": rule.get("校验类型"),
                "适用数据源": rule.get("适用数据源"),
                "来源": "质量校验规则",
            }
            best_specificity = specificity
    if best is None:
        return {
            "阈值": default,
            "规则ID": None,
            "规则名称": f"{metric}默认阈值",
            "校验类型": metric,
            "适用数据源": "全部",
            "来源": "系统默认",
        }
    return best


# ---------------------------------------------------------------------------
# 试解析与质量判定
# ---------------------------------------------------------------------------

def list_samples(source_id: str | None = None, fmt: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("_parser_samples"))
    if source_id:
        rows = [row for row in rows if row.get("来源标识") == source_id]
    if fmt:
        rows = [row for row in rows if row.get("输入格式") == fmt]
    return rows


def create_sample(values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    required = ["样本编号", "来源标识", "输入格式", "原始正文"]
    missing = [key for key in required if not str(values.get(key) or "").strip()]
    if missing:
        return None, missing
    expected = values.get("期望字段") or {}
    if isinstance(expected, str):
        parsed = _json_or_none(expected)
        expected = parsed if isinstance(parsed, dict) else {}
    row = {
        "id": _next_id("_parser_samples"),
        "样本编号": str(values["样本编号"]).strip(),
        "来源标识": str(values["来源标识"]).strip(),
        "输入格式": str(values["输入格式"]).strip(),
        "原始正文": str(values["原始正文"]),
        "期望字段": expected,
        "备注": values.get("备注", ""),
        "采集时间": values.get("采集时间") or now_iso(),
    }
    store.rows("_parser_samples").append(row)
    return row, []


def list_candidates(source_id: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("_parser_candidates"))
    if source_id:
        rows = [row for row in rows if row.get("来源标识") == source_id]
    return rows


def find_candidate(candidate_id: str | int | None) -> dict[str, Any] | None:
    if candidate_id is None:
        return None
    text = str(candidate_id).strip()
    for row in store.rows("_parser_candidates"):
        if text in {str(row.get("id")), str(row.get("候选编号"))}:
            return row
    return None


def create_candidate(values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    source = str(values.get("来源标识") or "").strip()
    rule_set = str(values.get("规则集名称") or "").strip()
    version = str(values.get("规则集版本") or "").strip()
    formats = values.get("有意纳入格式") or []
    definitions = values.get("规则定义") or {}
    missing = []
    if not source:
        missing.append("来源标识")
    if not rule_set:
        missing.append("规则集名称")
    if not version:
        missing.append("规则集版本")
    if not formats:
        missing.append("有意纳入格式")
    if not isinstance(definitions, dict) or any(fmt not in definitions for fmt in formats):
        missing.append("规则定义")
    if missing:
        return None, missing

    normalized: dict[str, dict[str, Any]] = {}
    for fmt in formats:
        spec = definitions.get(fmt) or {}
        if isinstance(spec, str):
            spec = {"输出字段列表": spec}
        rule_name = str(spec.get("规则名称") or f"{rule_set}-{fmt}").strip()
        normalized[fmt] = {
            "规则名称": rule_name,
            "输出字段列表": str(spec.get("输出字段列表") or ""),
            "解析引擎": str(spec.get("解析引擎") or ""),
            "字段映射": spec.get("字段映射") or {},
        }

    mandatory = values.get("必填字段") or []
    if isinstance(mandatory, str):
        mandatory = [item.strip() for item in mandatory.split(",") if item.strip()]

    candidate_no = str(values.get("候选编号") or "").strip()
    if not candidate_no:
        candidate_no = f"CAND-{len(store.rows('_parser_candidates')) + 1:04d}"
    for existing in store.rows("_parser_candidates"):
        if existing.get("候选编号") == candidate_no:
            return None, ["候选编号（已存在）"]

    row = {
        "id": _next_id("_parser_candidates"),
        "候选编号": candidate_no,
        "来源标识": source,
        "规则集名称": rule_set,
        "规则集版本": version,
        "必填字段": list(mandatory),
        "有意纳入格式": list(formats),
        "规则定义": normalized,
        "状态": "待试解析",
        "创建时间": values.get("创建时间") or now_iso(),
        "说明": values.get("说明", ""),
    }
    store.rows("_parser_candidates").append(row)
    return row, []


def _evaluate_sample(
    sample: dict[str, Any],
    rule: dict[str, Any],
    required: list[str],
) -> dict[str, Any]:
    expected_raw = sample.get("期望字段") or {}
    if isinstance(expected_raw, str):
        parsed_expected = _json_or_none(expected_raw)
        expected = parsed_expected if isinstance(parsed_expected, dict) else {}
    else:
        expected = dict(expected_raw)

    error = ""
    try:
        parsed = parse_body(sample.get("原始正文", ""), rule)
    except Exception as exc:  # 单条样本解析异常不能中断整批试解析
        parsed = {}
        error = str(exc) or exc.__class__.__name__

    missing_fields = [
        field for field in required
        if not str(parsed.get(field, "") or "").strip()
    ]
    misaligned_fields: list[str] = []
    value_mismatches: list[str] = []
    for field, expected_value in expected.items():
        expected_text = str(expected_value or "").strip()
        if not expected_text:
            continue
        actual_text = str(parsed.get(field, "") or "").strip()
        if not actual_text or actual_text == expected_text:
            continue
        # 值出现在别的期望字段位置上，说明是整列错位而不是单纯的值错误。
        shifted = any(
            actual_text == str(other_value or "").strip()
            for other_field, other_value in expected.items()
            if other_field != field and str(other_value or "").strip()
        )
        if shifted:
            misaligned_fields.append(field)
        else:
            value_mismatches.append(field)

    return {
        "样本编号": sample.get("样本编号"),
        "原始正文": sample.get("原始正文", ""),
        "解析字段": parsed,
        "期望字段": expected,
        "缺少字段": missing_fields,
        "错列字段": misaligned_fields,
        "值不符字段": value_mismatches,
        "解析异常": error,
    }


def evaluate_format(
    source: str,
    fmt: str,
    candidate: dict[str, Any],
    *,
    details: bool = True,
) -> dict[str, Any]:
    """按单个输入格式独立试解析并判定，阈值也按该格式单独解析。"""
    rule = (candidate.get("规则定义") or {}).get(fmt) or {}
    required = _required_fields(candidate)
    samples = list_samples(source, fmt)
    missing_threshold = resolve_threshold(MISSING_METRIC, source, fmt)
    misaligned_threshold = resolve_threshold(MISALIGNED_METRIC, source, fmt)

    detail_rows = [_evaluate_sample(sample, rule, required) for sample in samples]
    total = len(detail_rows)
    missing_records = sum(1 for row in detail_rows if row["缺少字段"])
    misaligned_records = sum(1 for row in detail_rows if row["错列字段"])
    missing_rate = round(missing_records / total, 4) if total else 0.0
    misaligned_rate = round(misaligned_records / total, 4) if total else 0.0

    missing_stats: dict[str, int] = {}
    misaligned_stats: dict[str, int] = {}
    mismatch_stats: dict[str, int] = {}
    for row in detail_rows:
        for field in row["缺少字段"]:
            missing_stats[field] = missing_stats.get(field, 0) + 1
        for field in row["错列字段"]:
            misaligned_stats[field] = misaligned_stats.get(field, 0) + 1
        for field in row["值不符字段"]:
            mismatch_stats[field] = mismatch_stats.get(field, 0) + 1

    exceeded: list[dict[str, Any]] = []
    if total == 0:
        exceeded.append({"指标": "样本", "实际": 0, "阈值": None, "说明": "该格式没有可用样本"})
    if missing_rate > missing_threshold["阈值"]:
        exceeded.append({
            "指标": MISSING_METRIC,
            "实际": missing_rate,
            "阈值": missing_threshold["阈值"],
            "说明": f"{missing_records}/{total} 条样本缺少必填字段",
        })
    if misaligned_rate > misaligned_threshold["阈值"]:
        exceeded.append({
            "指标": MISALIGNED_METRIC,
            "实际": misaligned_rate,
            "阈值": misaligned_threshold["阈值"],
            "说明": f"{misaligned_records}/{total} 条样本出现整列错位",
        })

    if total == 0:
        verdict = "无样本"
    elif exceeded:
        verdict = "不合格"
    else:
        verdict = "合格"

    result: dict[str, Any] = {
        "输入格式": fmt,
        "规则名称": rule.get("规则名称", ""),
        "输出字段列表": rule.get("输出字段列表", ""),
        "解析引擎": rule.get("解析引擎", ""),
        "样本数": total,
        "缺字段率": missing_rate,
        "错列率": misaligned_rate,
        "缺字段阈值": missing_threshold["阈值"],
        "错列阈值": misaligned_threshold["阈值"],
        "阈值来源": {
            MISSING_METRIC: missing_threshold,
            MISALIGNED_METRIC: misaligned_threshold,
        },
        "判定": verdict,
        "超限项": exceeded,
        "字段缺失统计": missing_stats,
        "错列字段统计": misaligned_stats,
        "值不符统计": mismatch_stats,
        "缺字段记录数": missing_records,
        "错列记录数": misaligned_records,
    }
    if details:
        result["样本明细"] = detail_rows
    return result


def _candidate_snapshot(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "候选编号": candidate.get("候选编号"),
        "来源标识": candidate.get("来源标识"),
        "规则集名称": candidate.get("规则集名称"),
        "规则集版本": candidate.get("规则集版本"),
        "必填字段": list(candidate.get("必填字段") or []),
        "有意纳入格式": list(candidate.get("有意纳入格式") or []),
        "规则定义": json.loads(json.dumps(candidate.get("规则定义") or {}, ensure_ascii=False)),
    }


def run_trial(candidate_id: str | int) -> tuple[dict[str, Any] | None, str]:
    candidate = find_candidate(candidate_id)
    if candidate is None:
        return None, f"候选 {candidate_id} 不存在"

    formats = list(candidate.get("有意纳入格式") or [])
    if not formats:
        return None, "候选没有声明有意纳入的格式"

    results: dict[str, Any] = {}
    for fmt in formats:
        results[fmt] = evaluate_format(str(candidate.get("来源标识")), fmt, candidate)

    passed = [fmt for fmt, result in results.items() if result["判定"] == "合格"]
    failed = [fmt for fmt, result in results.items() if result["判定"] != "合格"]
    if not failed:
        overall = "合格"
    elif passed:
        overall = "部分合格"
    else:
        overall = "不合格"

    trial = {
        "id": _next_id("_parser_trials"),
        "试解析编号": "",
        "候选编号": candidate.get("候选编号"),
        "来源标识": candidate.get("来源标识"),
        "规则集名称": candidate.get("规则集名称"),
        "规则集版本": candidate.get("规则集版本"),
        "候选快照": _candidate_snapshot(candidate),
        "格式结果": {fmt: {key: value for key, value in result.items() if key != "样本明细"}
                     for fmt, result in results.items()},
        "样本明细": {fmt: result.get("样本明细", []) for fmt, result in results.items()},
        "已合格格式": passed,
        "不合格格式": failed,
        "判定": overall,
        "试解析时间": now_iso(),
    }
    trial["试解析编号"] = f"TRIAL-{trial['id']:04d}"
    store.rows("_parser_trials").append(trial)

    candidate["状态"] = f"已试解析-{overall}"
    candidate["最近试解析编号"] = trial["试解析编号"]
    candidate["最近试解析时间"] = trial["试解析时间"]
    _journal("试解析", trial["试解析编号"], "完成", {
        "候选编号": candidate.get("候选编号"),
        "判定": overall,
        "已合格格式": passed,
        "不合格格式": failed,
        "格式结果": trial["格式结果"],
    })
    return trial, ""


def list_trials(candidate_id: str | int | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("_parser_trials"))
    if candidate_id is not None:
        text = str(candidate_id).strip()
        candidate = find_candidate(candidate_id)
        candidate_no = candidate.get("候选编号") if candidate else text
        rows = [row for row in rows if str(row.get("候选编号")) == str(candidate_no)]
    return rows


def find_trial(trial_id: str | int | None) -> dict[str, Any] | None:
    if trial_id is None:
        return None
    text = str(trial_id).strip()
    for row in store.rows("_parser_trials"):
        if text in {str(row.get("id")), str(row.get("试解析编号"))}:
            return row
    return None


def _recompute_trial(trial: dict[str, Any]) -> dict[str, Any]:
    """用已保存的样本解析结果按当前阈值重算，不重新解析样本。"""
    snapshot = trial.get("候选快照") or {}
    source = str(snapshot.get("来源标识") or trial.get("来源标识") or "")
    required = snapshot.get("必填字段") or []
    formats = snapshot.get("有意纳入格式") or list((trial.get("格式结果") or {}).keys())

    passed: list[str] = []
    failed: list[str] = []
    for fmt in formats:
        rule = (snapshot.get("规则定义") or {}).get(fmt) or {}
        details = (trial.get("样本明细") or {}).get(fmt) or []
        missing_threshold = resolve_threshold(MISSING_METRIC, source, fmt)
        misaligned_threshold = resolve_threshold(MISALIGNED_METRIC, source, fmt)

        total = len(details)
        missing_records = 0
        misaligned_records = 0
        missing_stats: dict[str, int] = {}
        misaligned_stats: dict[str, int] = {}
        mismatch_stats: dict[str, int] = {}
        for row in details:
            # 用已保存的解析结果和当前必填字段重新判定，确保阈值/必填口径变化
            # 后重算的是新口径而不是沿用旧结论。
            parsed_fields = row.get("解析字段") or {}
            missing_fields = [
                field for field in required
                if not str(parsed_fields.get(field, "") or "").strip()
            ]
            if missing_fields:
                missing_records += 1
            for field in missing_fields:
                missing_stats[field] = missing_stats.get(field, 0) + 1
            if row.get("错列字段"):
                misaligned_records += 1
            for field in row.get("错列字段") or []:
                misaligned_stats[field] = misaligned_stats.get(field, 0) + 1
            for field in row.get("值不符字段") or []:
                mismatch_stats[field] = mismatch_stats.get(field, 0) + 1

        missing_rate = round(missing_records / total, 4) if total else 0.0
        misaligned_rate = round(misaligned_records / total, 4) if total else 0.0
        exceeded: list[dict[str, Any]] = []
        if total == 0:
            exceeded.append({"指标": "样本", "实际": 0, "阈值": None, "说明": "该格式没有可用样本"})
        if missing_rate > missing_threshold["阈值"]:
            exceeded.append({
                "指标": MISSING_METRIC,
                "实际": missing_rate,
                "阈值": missing_threshold["阈值"],
                "说明": f"{missing_records}/{total} 条样本缺少必填字段",
            })
        if misaligned_rate > misaligned_threshold["阈值"]:
            exceeded.append({
                "指标": MISALIGNED_METRIC,
                "实际": misaligned_rate,
                "阈值": misaligned_threshold["阈值"],
                "说明": f"{misaligned_records}/{total} 条样本出现整列错位",
            })
        if total == 0:
            verdict = "无样本"
        elif exceeded:
            verdict = "不合格"
        else:
            verdict = "合格"
        if verdict == "合格":
            passed.append(fmt)
        else:
            failed.append(fmt)

        trial.setdefault("格式结果", {})[fmt] = {
            "输入格式": fmt,
            "规则名称": rule.get("规则名称", ""),
            "输出字段列表": rule.get("输出字段列表", ""),
            "解析引擎": rule.get("解析引擎", ""),
            "样本数": total,
            "缺字段率": missing_rate,
            "错列率": misaligned_rate,
            "缺字段阈值": missing_threshold["阈值"],
            "错列阈值": misaligned_threshold["阈值"],
            "阈值来源": {
                MISSING_METRIC: missing_threshold,
                MISALIGNED_METRIC: misaligned_threshold,
            },
            "判定": verdict,
            "超限项": exceeded,
            "字段缺失统计": missing_stats,
            "错列字段统计": misaligned_stats,
            "值不符统计": mismatch_stats,
            "缺字段记录数": missing_records,
            "错列记录数": misaligned_records,
        }

    trial["已合格格式"] = passed
    trial["不合格格式"] = failed
    if not failed:
        trial["判定"] = "合格"
    elif passed:
        trial["判定"] = "部分合格"
    else:
        trial["判定"] = "不合格"
    trial["最近重算时间"] = now_iso()
    return trial


# ---------------------------------------------------------------------------
# 发布清单
# ---------------------------------------------------------------------------

def _rules_version(candidate: dict[str, Any], fmt: str, manifest_id: int) -> str:
    return (
        f"{candidate.get('规则集版本') or 'v1'}"
        f"/{fmt}"
        f"/r{manifest_id}"
    )


def _journal(event: str, reference: str, status: str, detail: dict[str, Any]) -> dict[str, Any]:
    row = {
        "id": _next_id("_publish_journal"),
        "事件": event,
        "关联编号": reference,
        "状态": status,
        "详情": detail,
        "时间": now_iso(),
    }
    store.rows("_publish_journal").append(row)
    return row


def list_manifests(source_id: str | None = None) -> list[dict[str, Any]]:
    rows = list(store.rows("_parser_manifests"))
    if source_id:
        rows = [row for row in rows if row.get("来源标识") == source_id]
    return rows


def active_manifest(source_id: str) -> dict[str, Any] | None:
    rows = [
        row for row in store.rows("_parser_manifests")
        if row.get("来源标识") == source_id and row.get("当前生效")
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: int(row.get("id", 0)))


def _staged_rules(manifest: dict[str, Any], fmt: str) -> list[dict[str, Any]]:
    token = manifest.get("清单编号")
    return [
        rule for rule in store.rows("field_parser")
        if rule.get("发布清单") == token
        and rule.get("输入格式") == fmt
        and rule.get("status") == "草稿"
    ]


def _snapshot_rule(candidate: dict[str, Any], fmt: str, manifest: dict[str, Any]) -> dict[str, Any]:
    spec = (candidate.get("规则定义") or {}).get(fmt) or {}
    return {
        "规则名称": spec.get("规则名称", ""),
        "输入格式": fmt,
        "输出字段列表": spec.get("输出字段列表", ""),
        "解析引擎": spec.get("解析引擎", ""),
        "字段映射": spec.get("字段映射") or {},
        "必填字段": list(candidate.get("必填字段") or []),
        "规则版本": "",
        "规则集名称": candidate.get("规则集名称"),
        "规则集版本": candidate.get("规则集版本"),
        "发布清单": manifest.get("清单编号"),
    }


def _stage_format(manifest: dict[str, Any], candidate: dict[str, Any], fmt: str) -> dict[str, Any]:
    """暂存一个格式的规则：先落草稿，提交时才切换为已发布。"""
    existing = _staged_rules(manifest, fmt)
    if existing:
        rule = existing[0]
    else:
        version = _rules_version(candidate, fmt, int(manifest.get("id", 0)))
        spec = (candidate.get("规则定义") or {}).get(fmt) or {}
        rule = {
            "id": _next_id("field_parser"),
            "规则名称": spec.get("规则名称", ""),
            "适用数据源": candidate.get("来源标识"),
            "输入格式": fmt,
            "输出字段列表": spec.get("输出字段列表", ""),
            "解析引擎": spec.get("解析引擎", ""),
            "字段映射": spec.get("字段映射") or {},
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "规则版本": version,
            "规则集名称": candidate.get("规则集名称"),
            "规则集版本": candidate.get("规则集版本"),
            "候选编号": candidate.get("候选编号"),
            "发布清单": manifest.get("清单编号"),
            "暂存": True,
            "创建时间": now_iso(),
        }
        store.rows("field_parser").append(rule)
        _journal("暂存规则", manifest.get("清单编号", ""), "暂存", {
            "输入格式": fmt,
            "规则名称": rule.get("规则名称"),
            "规则版本": version,
        })
    if fmt not in manifest.setdefault("已暂存格式", []):
        manifest["已暂存格式"].append(fmt)
    manifest.setdefault("规则快照", {})[fmt] = {
        **_snapshot_rule(candidate, fmt, manifest),
        "规则版本": rule.get("规则版本"),
        "规则ID": rule.get("id"),
    }
    return rule


def _commit_manifest(manifest: dict[str, Any], candidate: dict[str, Any]) -> None:
    """原子提交：先完成全部格式的规则切换，最后再让清单生效。"""
    source = str(manifest.get("来源标识"))
    accepted = list(manifest.get("待提交格式") or [])
    published_rules: dict[str, Any] = {}

    for fmt in accepted:
        staged = _staged_rules(manifest, fmt)
        if not staged:
            # 理论上不会发生：恢复时应先补齐暂存；防御性补一次。
            _stage_format(manifest, candidate, fmt)
            staged = _staged_rules(manifest, fmt)
        rule = staged[0]
        # 同源同格式的旧规则不再作为生产映射，但保留行用于按原版本回查。
        for old in store.rows("field_parser"):
            if old is rule:
                continue
            if old.get("适用数据源") != source or old.get("输入格式") != fmt:
                continue
            if old.get("status") == "已发布":
                old["status"] = "已弃用"
                old["pending"] = False
                old["当前生效"] = False
                old["被替代版本"] = rule.get("规则版本")
        rule["status"] = "已发布"
        rule["pending"] = False
        rule["暂存"] = False
        rule["当前生效"] = True
        rule["发布时间"] = now_iso()
        published_rules[fmt] = {
            "规则ID": rule.get("id"),
            "规则名称": rule.get("规则名称"),
            "规则版本": rule.get("规则版本"),
            "输出字段列表": rule.get("输出字段列表"),
            "解析引擎": rule.get("解析引擎"),
            "字段映射": rule.get("字段映射") or {},
            "规则集名称": rule.get("规则集名称"),
            "规则集版本": rule.get("规则集版本"),
        }

    # 旧清单在提交的最后一步才失效，保证提交前生产仍读到旧映射。
    for previous in store.rows("_parser_manifests"):
        if previous is manifest:
            continue
        if previous.get("来源标识") == source and previous.get("当前生效"):
            previous["当前生效"] = False
            previous["状态"] = "已被替代"
            previous["被替代清单"] = manifest.get("清单编号")

    manifest["已发布格式"] = accepted
    manifest["接受格式"] = accepted
    manifest["拒绝格式"] = [
        fmt for fmt in manifest.get("有意纳入格式", []) if fmt not in accepted
    ]
    manifest["已发布规则"] = published_rules
    manifest["状态"] = "已发布"
    manifest["当前生效"] = True
    manifest["发布时间"] = now_iso()
    manifest["发布时间线"] = manifest.get("发布时间线", []) + [
        {"事件": "提交", "时间": now_iso()}
    ]
    candidate["状态"] = "已发布"
    candidate["发布清单"] = manifest.get("清单编号")
    candidate["支持格式"] = accepted
    _journal("提交清单", manifest.get("清单编号", ""), "成功", {
        "来源标识": source,
        "接受格式": accepted,
        "拒绝格式": manifest.get("拒绝格式"),
        "已发布规则": published_rules,
    })


def publish_candidate(
    candidate_id: str | int,
    *,
    simulate_interrupt: bool = False,
) -> tuple[dict[str, Any] | None, str]:
    candidate = find_candidate(candidate_id)
    if candidate is None:
        return None, f"候选 {candidate_id} 不存在"

    trial, error = run_trial(candidate.get("候选编号"))
    if trial is None:
        return None, error

    accepted = list(trial.get("已合格格式") or [])
    intended = list(trial.get("候选快照", {}).get("有意纳入格式") or candidate.get("有意纳入格式") or [])
    rejected = [fmt for fmt in intended if fmt not in accepted]

    if not accepted:
        manifest = _create_manifest(candidate, trial, intended, accepted)
        manifest["状态"] = "已阻断"
        manifest["当前生效"] = False
        manifest["阻断原因"] = "所有有意纳入格式的试解析结果均超限"
        candidate["状态"] = "已阻断"
        candidate["发布清单"] = manifest.get("清单编号")
        _journal("发布阻断", manifest.get("清单编号", ""), "阻断", {
            "候选编号": candidate.get("候选编号"),
            "试解析编号": trial.get("试解析编号"),
            "不合格格式": rejected,
        })
        return manifest, f"候选未通过任何格式的质量门槛，已阻断：{'、'.join(rejected)}"

    manifest = _create_manifest(candidate, trial, intended, accepted)
    manifest["状态"] = "发布中"
    manifest["当前生效"] = False
    manifest["已发布格式"] = []
    manifest["接受格式"] = []
    manifest["拒绝格式"] = rejected
    manifest["待提交格式"] = accepted

    staged_count = 0
    for fmt in accepted:
        _stage_format(manifest, candidate, fmt)
        staged_count += 1
        if simulate_interrupt and staged_count == 1:
            manifest["状态"] = "发布中断"
            manifest["中断位置"] = fmt
            manifest["中断时间"] = now_iso()
            candidate["状态"] = "发布中断"
            candidate["发布清单"] = manifest.get("清单编号")
            _journal("发布中断", manifest.get("清单编号", ""), "中断", {
                "候选编号": candidate.get("候选编号"),
                "已完成暂存格式": list(manifest.get("已暂存格式") or []),
                "待暂存格式": [item for item in accepted if item not in manifest.get("已暂存格式", [])],
                "说明": "清单未生效，生产仍使用上一份规则",
            })
            return manifest, (
                f"发布在暂存格式 {fmt} 后中断，清单未生效；"
                "恢复发布会从断点继续，不会重复生成规则版本"
            )

    _commit_manifest(manifest, candidate)
    return manifest, f"候选已发布，支持格式：{'、'.join(accepted)}"


def _create_manifest(
    candidate: dict[str, Any],
    trial: dict[str, Any],
    intended: list[str],
    accepted: list[str],
) -> dict[str, Any]:
    manifest = {
        "id": _next_id("_parser_manifests"),
        "来源标识": candidate.get("来源标识"),
        "候选编号": candidate.get("候选编号"),
        "规则集名称": candidate.get("规则集名称"),
        "规则集版本": candidate.get("规则集版本"),
        "试解析编号": trial.get("试解析编号"),
        "有意纳入格式": list(intended),
        "接受格式": list(accepted),
        "拒绝格式": [],
        "已发布格式": [],
        "已暂存格式": [],
        "格式判定": json.loads(json.dumps(trial.get("格式结果") or {}, ensure_ascii=False)),
        "规则快照": {},
        "已发布规则": {},
        "状态": "发布中",
        "当前生效": False,
        "创建时间": now_iso(),
        "阈值口径": {
            fmt: {
                MISSING_METRIC: resolve_threshold(MISSING_METRIC, candidate.get("来源标识"), fmt),
                MISALIGNED_METRIC: resolve_threshold(MISALIGNED_METRIC, candidate.get("来源标识"), fmt),
            }
            for fmt in intended
        },
    }
    manifest["清单编号"] = f"MAN-{manifest['id']:04d}"
    store.rows("_parser_manifests").append(manifest)
    _journal("创建清单", manifest["清单编号"], "创建", {
        "候选编号": candidate.get("候选编号"),
        "试解析编号": trial.get("试解析编号"),
        "有意纳入格式": intended,
        "待提交格式": accepted,
    })
    return manifest


def resume_publish(manifest_id: str | int) -> tuple[dict[str, Any] | None, str]:
    manifest = None
    for row in store.rows("_parser_manifests"):
        if str(row.get("id")) == str(manifest_id) or row.get("清单编号") == str(manifest_id):
            manifest = row
            break
    if manifest is None:
        return None, f"发布清单 {manifest_id} 不存在"
    if manifest.get("状态") != "发布中断":
        return None, f"发布清单 {manifest.get('清单编号')} 当前状态为 {manifest.get('状态')}，无需恢复"

    candidate = find_candidate(manifest.get("候选编号"))
    if candidate is None:
        return None, f"清单关联的候选 {manifest.get('候选编号')} 不存在"
    pending = list(manifest.get("待提交格式") or [])
    for fmt in pending:
        _stage_format(manifest, candidate, fmt)
    _journal("恢复发布", manifest.get("清单编号", ""), "继续", {
        "补齐格式": [fmt for fmt in pending if fmt not in manifest.get("已暂存格式", [])],
        "已暂存格式": list(manifest.get("已暂存格式") or []),
    })
    _commit_manifest(manifest, candidate)
    return manifest, f"发布已从中断点恢复，支持格式：{'、'.join(manifest.get('接受格式') or [])}"


def eligibility(source_id: str, fmt: str) -> dict[str, Any]:
    manifest = active_manifest(source_id)
    if manifest is None:
        return {
            "来源标识": source_id,
            "输入格式": fmt,
            "可生产解析": False,
            "原因": "该来源尚未发布任何解析清单，不能用于生产日志",
            "清单编号": None,
        }
    if fmt in (manifest.get("接受格式") or []):
        rule = (manifest.get("已发布规则") or {}).get(fmt) or {}
        return {
            "来源标识": source_id,
            "输入格式": fmt,
            "可生产解析": True,
            "原因": "该格式已通过发布清单质量门槛",
            "清单编号": manifest.get("清单编号"),
            "规则集名称": manifest.get("规则集名称"),
            "规则集版本": manifest.get("规则集版本"),
            "规则": rule,
        }
    verdict = (manifest.get("格式判定") or {}).get(fmt) or {}
    return {
        "来源标识": source_id,
        "输入格式": fmt,
        "可生产解析": False,
        "原因": "该格式未通过发布清单质量门槛，禁止用于该类生产日志",
        "清单编号": manifest.get("清单编号"),
        "规则集名称": manifest.get("规则集名称"),
        "规则集版本": manifest.get("规则集版本"),
        "判定": verdict.get("判定"),
        "超限项": verdict.get("超限项", []),
        "字段缺失统计": verdict.get("字段缺失统计", {}),
        "错列字段统计": verdict.get("错列字段统计", {}),
    }


def eligibility_map(source_id: str) -> dict[str, dict[str, Any]]:
    manifest = active_manifest(source_id)
    if manifest is None:
        return {}
    return {
        fmt: eligibility(source_id, fmt)
        for fmt in manifest.get("有意纳入格式", [])
    }


def gate_enabled(source_id: str) -> bool:
    """来源是否已经纳入质量门槛管理。

    只要该来源登记过样本、候选或发布清单，就视为门槛已启用：
    此后没有生效清单时所有格式都不能用于生产解析。完全没有门槛配置的
    历史来源继续走原来的解析路径，避免影响既有接入。
    """
    if not source_id:
        return False
    if any(row.get("来源标识") == source_id for row in store.rows("_parser_samples")):
        return True
    if any(row.get("来源标识") == source_id for row in store.rows("_parser_candidates")):
        return True
    if any(row.get("来源标识") == source_id for row in store.rows("_parser_manifests")):
        return True
    return False


# ---------------------------------------------------------------------------
# 阈值变更与重算
# ---------------------------------------------------------------------------

def _threshold_rule(metric: str, source: str | None, fmt: str | None) -> dict[str, Any]:
    label = f"{metric}门槛[{source or '*'}:{fmt or '*'}]"
    for rule in store.rows("quality_check"):
        if rule.get("规则名称") == label:
            return rule
    rule = {
        "id": _next_id("quality_check"),
        "规则名称": label,
        "校验类型": metric,
        "阈值参数": "",
        "适用数据源": source or "全部",
        "严重级别": "高",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "创建时间": now_iso(),
    }
    store.rows("quality_check").append(rule)
    return rule


def update_thresholds(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """更新质量阈值并按新口径重算所有已试验样本。"""
    source = str(payload.get("来源标识") or "").strip() or None
    fmt = str(payload.get("输入格式") or "").strip() or None
    updated: list[dict[str, Any]] = []

    direct_rule_id = payload.get("质量规则ID") or payload.get("rule_id")
    if direct_rule_id is not None:
        target = None
        for rule in store.rows("quality_check"):
            if str(rule.get("id")) == str(direct_rule_id):
                target = rule
                break
        if target is None:
            return None, f"质量校验规则 {direct_rule_id} 不存在"
        threshold = payload.get("阈值参数")
        if threshold is None:
            threshold = payload.get("阈值")
        if threshold is None:
            return None, "缺少阈值参数"
        target["阈值参数"] = threshold
        target["status"] = "已生效"
        target["pending"] = False
        updated.append({"规则名称": target.get("规则名称"), "阈值参数": threshold})
    else:
        pairs = (
            (MISSING_METRIC, payload.get("缺字段率", payload.get("missing_rate"))),
            (MISALIGNED_METRIC, payload.get("错列率", payload.get("misaligned_rate"))),
        )
        for metric, value in pairs:
            if value is None:
                continue
            number = _float_or_none(value)
            if number is None or number < 0:
                return None, f"{metric}阈值必须是非负数"
            rule = _threshold_rule(metric, source, fmt)
            if fmt:
                rule["阈值参数"] = json.dumps(
                    {"输入格式": fmt, "阈值": number}, ensure_ascii=False)
            else:
                rule["阈值参数"] = number
            rule["status"] = "已生效"
            rule["pending"] = False
            updated.append({"规则名称": rule.get("规则名称"), "阈值参数": rule.get("阈值参数")})
        if not updated:
            return None, "未提供需要变更的阈值"

    summary = recompute_all(reason="阈值变更")
    _journal("阈值变更", "质量校验规则", "完成", {"变更": updated, "重算": summary})
    return {
        "更新": updated,
        "重算": summary,
    }, "阈值已更新，已按新口径重算已试验样本"


def recompute_all(reason: str = "手动重算") -> dict[str, Any]:
    """重新判定所有试解析记录，并同步更新发布清单的可用范围。"""
    trials = list(store.rows("_parser_trials"))
    trial_changes: list[dict[str, Any]] = []
    for trial in trials:
        before = (trial.get("判定"), list(trial.get("已合格格式") or []))
        _recompute_trial(trial)
        after = (trial.get("判定"), list(trial.get("已合格格式") or []))
        if before != after:
            trial_changes.append({
                "试解析编号": trial.get("试解析编号"),
                "候选编号": trial.get("候选编号"),
                "原判定": before[0],
                "新判定": after[0],
                "原合格格式": before[1],
                "新合格格式": after[1],
            })

    manifest_changes: list[dict[str, Any]] = []
    for manifest in store.rows("_parser_manifests"):
        trial = find_trial(manifest.get("试解析编号"))
        if trial is None:
            continue
        before_accepted = list(manifest.get("接受格式") or [])
        published = list(manifest.get("已发布格式") or [])
        passed = [
            fmt for fmt in (manifest.get("有意纳入格式") or [])
            if fmt in (trial.get("已合格格式") or [])
        ]
        # 阈值变化只能收紧或恢复“已经实际发布过”的格式；在发布时被判不合格的格式
        # 没有对应生产规则，不能因为阈值放宽就自动可用。
        accepted = [fmt for fmt in published if fmt in passed]
        manifest["格式判定"] = json.loads(
            json.dumps(trial.get("格式结果") or {}, ensure_ascii=False))
        manifest["接受格式"] = accepted
        manifest["拒绝格式"] = [
            fmt for fmt in manifest.get("有意纳入格式", []) if fmt not in accepted
        ]
        manifest["阈值口径"] = {
            fmt: {
                MISSING_METRIC: resolve_threshold(MISSING_METRIC, manifest.get("来源标识"), fmt),
                MISALIGNED_METRIC: resolve_threshold(MISALIGNED_METRIC, manifest.get("来源标识"), fmt),
            }
            for fmt in manifest.get("有意纳入格式", [])
        }
        manifest["最近重算时间"] = now_iso()
        if before_accepted != accepted and manifest.get("当前生效"):
            manifest_changes.append({
                "清单编号": manifest.get("清单编号"),
                "来源标识": manifest.get("来源标识"),
                "原接受格式": before_accepted,
                "新接受格式": accepted,
                "被阻断格式": [fmt for fmt in before_accepted if fmt not in accepted],
            })

    summary = {
        "重算时间": now_iso(),
        "原因": reason,
        "试解析数": len(trials),
        "判定变化": trial_changes,
        "清单变化": manifest_changes,
    }
    if trials or manifest_changes:
        _journal("重算样本", reason, "完成", summary)
    return summary


# ---------------------------------------------------------------------------
# 版本回查与同源合计
# ---------------------------------------------------------------------------

def list_rule_versions(source_id: str | None = None) -> list[dict[str, Any]]:
    rows = []
    for rule in store.rows("field_parser"):
        version = rule.get("规则版本")
        if not version:
            continue
        if source_id and rule.get("适用数据源") != source_id:
            continue
        rows.append({
            "规则ID": rule.get("id"),
            "规则名称": rule.get("规则名称"),
            "规则版本": version,
            "来源标识": rule.get("适用数据源"),
            "输入格式": rule.get("输入格式"),
            "规则集名称": rule.get("规则集名称"),
            "规则集版本": rule.get("规则集版本"),
            "状态": rule.get("status"),
            "当前生效": bool(rule.get("当前生效")),
            "输出字段列表": rule.get("输出字段列表"),
            "解析引擎": rule.get("解析引擎"),
        })
    return sorted(rows, key=lambda row: str(row.get("规则版本")))


def records_by_rule_version(
    rule_version: str,
    source_id: str | None = None,
    fmt: str | None = None,
) -> dict[str, Any]:
    rows = [
        row for row in store.rows("parsed_entity")
        if str(row.get("解析规则版本")) == str(rule_version)
    ]
    if source_id:
        rows = [row for row in rows if row.get("来源标识") == source_id]
    if fmt:
        rows = [row for row in rows if row.get("输入格式") == fmt]
    rule = None
    for candidate in store.rows("field_parser"):
        if str(candidate.get("规则版本")) == str(rule_version):
            rule = {
                "规则ID": candidate.get("id"),
                "规则名称": candidate.get("规则名称"),
                "规则版本": candidate.get("规则版本"),
                "来源标识": candidate.get("适用数据源"),
                "输入格式": candidate.get("输入格式"),
                "输出字段列表": candidate.get("输出字段列表"),
                "解析引擎": candidate.get("解析引擎"),
                "字段映射": candidate.get("字段映射") or {},
                "规则集名称": candidate.get("规则集名称"),
                "规则集版本": candidate.get("规则集版本"),
                "状态": candidate.get("status"),
            }
            break
    return {
        "规则版本": rule_version,
        "规则": rule,
        "记录数": len(rows),
        "记录": rows,
    }


def source_stats(source_id: str) -> dict[str, Any]:
    entities = [row for row in store.rows("parsed_entity") if row.get("来源标识") == source_id]
    formats: dict[str, dict[str, Any]] = {}
    rule_sets: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in entities:
        fmt = str(row.get("输入格式") or "未标注格式")
        bucket = formats.setdefault(fmt, {
            "输入格式": fmt,
            "记录数": 0,
            "规则版本使用": {},
            "校验状态": {},
        })
        bucket["记录数"] += 1
        version = str(row.get("解析规则版本") or "未知版本")
        bucket["规则版本使用"][version] = bucket["规则版本使用"].get(version, 0) + 1
        state = str(row.get("校验状态") or row.get("status") or "未知")
        bucket["校验状态"][state] = bucket["校验状态"].get(state, 0) + 1
        key = (
            str(row.get("规则集名称") or "未标注规则集"),
            str(row.get("规则集版本") or "未标注版本"),
            fmt,
            version,
        )
        usage = rule_sets.setdefault(key, {
            "规则集名称": key[0],
            "规则集版本": key[1],
            "输入格式": fmt,
            "规则版本": version,
            "记录数": 0,
        })
        usage["记录数"] += 1

    manifest = active_manifest(source_id)
    current_label = None
    if manifest:
        current_label = f"{manifest.get('规则集名称')}@{manifest.get('规则集版本')}"
    total = len(entities)
    used = sorted(rule_sets.values(), key=lambda item: (
        str(item["规则集名称"]), str(item["规则集版本"]), str(item["输入格式"])))
    return {
        "来源标识": source_id,
        "记录数": total,
        "格式数": len(formats),
        "格式统计": formats,
        "使用的规则集": used,
        "当前生效规则集": {
            "清单编号": manifest.get("清单编号"),
            "规则集名称": manifest.get("规则集名称"),
            "规则集版本": manifest.get("规则集版本"),
            "接受格式": manifest.get("接受格式", []),
        } if manifest else None,
        "规则集说明": (
            f"同源合计 {total} 条，覆盖 {len(formats)} 种格式；"
            f"共使用 {len(used)} 套规则版本"
            + (f"；当前生效规则集为 {current_label}" if current_label else "；当前无生效规则集")
        ),
        "可生产格式": list((manifest or {}).get("接受格式") or []),
        "阻断格式": list((manifest or {}).get("拒绝格式") or []),
    }
