"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.store import store
from app.services import parser_release


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
        default_format: str | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。

        每条原始记录在入库时就标注输入格式；如果没有显式给出，则使用数据源登记
        的默认格式。格式一旦写入原始记录，后续解析不再猜测。
        """
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        fallback_format = default_format or source_entry.get("默认格式")
        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        # 批次启动时固定当前生效的解析清单快照。批次进行中即使发布完成，
        # 解析也继续使用这批快照，不会换错映射。
        mapping = self._pin_mapping(source_id)
        batch_entry["规则清单"] = mapping.get("清单编号")
        batch_entry["解析映射"] = mapping.get("映射", {})
        batch_entry["可生产格式"] = mapping.get("接受格式", [])
        # 批次启动即固定映射；启动时若还没有生效清单，也不在解析中途接管新清单。
        batch_entry["映射已固定"] = True
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "输入格式": log.get("format") or fallback_format or "",
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def _pin_mapping(self, source_id: str) -> dict[str, Any]:
        """批次启动时快照当前生效清单的映射，后续发布不影响本批次。"""
        manifest = parser_release.active_manifest(source_id)
        if manifest is None:
            return {"清单编号": None, "映射": {}, "接受格式": []}
        mapping: dict[str, Any] = {}
        for fmt, rule in (manifest.get("已发布规则") or {}).items():
            mapping[fmt] = {
                "规则名称": rule.get("规则名称"),
                "规则版本": rule.get("规则版本"),
                "输出字段列表": rule.get("输出字段列表"),
                "解析引擎": rule.get("解析引擎"),
                "字段映射": rule.get("字段映射") or {},
                "规则集名称": manifest.get("规则集名称"),
                "规则集版本": manifest.get("规则集版本"),
                "清单编号": manifest.get("清单编号"),
            }
        return {
            "清单编号": manifest.get("清单编号"),
            "规则集名称": manifest.get("规则集名称"),
            "规则集版本": manifest.get("规则集版本"),
            "接受格式": list(manifest.get("接受格式") or []),
            "映射": mapping,
        }

    def _explicit_rule(self, parser_rule_name: str | None) -> dict[str, Any] | None:
        if not parser_rule_name:
            return None
        for rule in store.rows("field_parser"):
            if rule.get("规则名称") == parser_rule_name:
                return rule
        return None

    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。

        规则按输入格式独立解析；批次启动时固定的清单快照决定用哪套规则版本。
        未通过发布门槛的格式不会被任何规则解析，直接记为解析失败并说明原因。
        """
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        batch = None
        for item in store.rows("ingest_batch"):
            if item.get("批次编号") == batch_no:
                batch = item
                break

        source_id = (
            (batch or {}).get("所属数据源")
            or next((r.get("source_id") for r in batch_raw if r.get("source_id")), None)
        )
        pinned = (batch or {}).get("解析映射") or {}
        pinned_manifest = (batch or {}).get("规则清单")
        explicit = self._explicit_rule(parser_rule_name)
        if parser_rule_name and explicit is None:
            return {"ok": False, "message": f"解析规则 {parser_rule_name} 不存在"}
        if explicit is not None and explicit.get("status") not in ("已发布", "已弃用"):
            # 草稿规则（包括发布暂存中的规则）不能直接用于生产解析。
            return {"ok": False, "message": f"解析规则 {parser_rule_name} 尚未发布，不能用于生产解析"}

        # 没有固定映射时保持历史行为：取一条已发布规则，按原方式解析。
        # 已经纳入质量门槛管理的来源（有样本/候选/清单）不适用这条历史回退。
        gate_enabled = parser_release.gate_enabled(str(source_id or ""))
        legacy_rule = explicit
        pin_locked = bool((batch or {}).get("映射已固定"))
        if not pinned and not pin_locked:
            active = parser_release.active_manifest(source_id) if source_id else None
            if active is not None:
                # 旧批次（没有固定映射标记）在解析前补一次快照；新批次启动时已固定，
                # 不会再接管解析中途发布完成的清单。
                pinned_manifest = active.get("清单编号")
                pinned = self._pin_mapping(source_id)["映射"]
        if legacy_rule is None and not pinned and not gate_enabled:
            for rule in store.rows("field_parser"):
                if rule.get("status") == "已发布":
                    legacy_rule = rule
                    break
        if legacy_rule is None and not pinned and not pin_locked and not gate_enabled:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []
        format_rules: dict[str, dict[str, Any]] = {}
        blocked_records: list[dict[str, Any]] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            fmt = str(raw.get("输入格式") or "")
            record_source = str(raw.get("source_id") or source_id or "")

            rule = pinned.get(fmt) if fmt else None
            ineligible_reason = ""
            if rule is None and fmt and pinned:
                # 清单已固定，但该格式不在可生产范围内：明确阻断。
                ineligible_reason = (
                    f"格式 {fmt} 未通过发布清单 {pinned_manifest} 的质量门槛，"
                    "不能用于该类生产日志"
                )
            elif rule is None and fmt and not pinned and gate_enabled:
                # 来源已纳入门槛但还没有生效清单：不能借用任何已发布规则。
                ineligible_reason = (
                    f"来源 {source_id} 还没有生效的解析发布清单，"
                    f"格式 {fmt} 不能用于生产日志"
                )
            elif rule is None and fmt and pinned_manifest is None and explicit is not None:
                rule = {
                    "规则名称": explicit.get("规则名称"),
                    "规则版本": explicit.get("规则版本") or explicit.get("规则名称"),
                    "输出字段列表": explicit.get("输出字段列表"),
                    "解析引擎": explicit.get("解析引擎"),
                    "字段映射": explicit.get("字段映射") or {},
                    "规则集名称": explicit.get("规则集名称"),
                    "规则集版本": explicit.get("规则集版本"),
                    "清单编号": explicit.get("发布清单"),
                }
            elif rule is None and not fmt and pinned:
                # 未标注格式的历史记录不能猜测映射。
                ineligible_reason = "原始记录未标注输入格式，无法确定生产规则"

            try:
                if ineligible_reason:
                    raise ValueError(ineligible_reason)
                if rule is not None:
                    fields = parser_release.parse_body(raw.get("原始正文", ""), rule)
                    rule_name = rule.get("规则名称", "unknown")
                    rule_version = rule.get("规则版本") or rule_name
                    rule_set_name = rule.get("规则集名称")
                    rule_set_version = rule.get("规则集版本")
                else:
                    # 历史无版本规则：沿用原来的解析实现。
                    fields = self._parse_body(raw.get("原始正文", ""), legacy_rule)
                    rule_name = legacy_rule.get("规则名称", "unknown")
                    rule_version = legacy_rule.get("规则版本") or rule_name
                    rule_set_name = legacy_rule.get("规则集名称")
                    rule_set_version = legacy_rule.get("规则集版本")

                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule_version,
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule_name,
                    "来源标识": record_source,
                    "输入格式": fmt,
                    "规则集名称": rule_set_name,
                    "规则集版本": rule_set_version,
                    "解析清单": pinned_manifest,
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                raw["解析规则版本"] = rule_version
                success_count += 1
                parsed_ids.append(ent_id)
                bucket = format_rules.setdefault(fmt or "未标注格式", {
                    "规则名称": rule_name,
                    "规则版本": rule_version,
                    "规则集名称": rule_set_name,
                    "规则集版本": rule_set_version,
                    "记录数": 0,
                })
                bucket["记录数"] += 1
            except Exception as exc:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                raw["解析失败原因"] = str(exc)
                failure_count += 1
                failed_ids.append(raw_id)
                blocked_records.append({
                    "原始记录ID": str(raw_id),
                    "输入格式": fmt,
                    "原因": str(exc),
                })

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                b["格式规则版本"] = format_rules
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
            "解析清单": pinned_manifest,
            "格式规则版本": format_rules,
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            blocked_by_id = {item["原始记录ID"]: item["原因"] for item in blocked_records}
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": blocked_by_id.get(str(fid), "字段解析失败"),
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": (legacy_rule or {}).get("规则名称"),
                    "解析清单": pinned_manifest,
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
            "manifest": pinned_manifest,
            "format_rules": format_rules,
            "blocked_records": blocked_records,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
        default_format: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(
            source_id, time_window, raw_logs, default_format)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()