"""解析发布门槛测试：按格式试解析、质量阈值判定、发布清单、中断恢复、
阈值重算、版本回查、同源合计和生产解析资格。"""
from __future__ import annotations

import json

import pytest

from app.services import parser_release
from app.store import store


def _rules_with_version():
    return [row for row in store.rows("field_parser") if row.get("规则版本")]


def _batch(batch_no):
    for row in store.rows("ingest_batch"):
        if row.get("批次编号") == batch_no:
            return row
    return None


class TestTrialParsing:
    """按来源格式试解析并计算缺字段率/错列率。"""

    def test_trial_judges_each_format_independently(self):
        trial, error = parser_release.run_trial("CAND-APP-0001")
        assert error == ""
        results = trial["格式结果"]
        assert results["app-json-v2"]["判定"] == "合格"
        assert results["app-text-v1"]["判定"] == "不合格"
        # 旧格式缺少主机名，且 message 整列错位
        assert results["app-text-v1"]["字段缺失统计"] == {"host": 4}
        assert results["app-text-v1"]["错列字段统计"] == {"message": 4}
        assert trial["判定"] == "部分合格"

    def test_missing_rate_and_misaligned_rate_are_computed_from_samples(self):
        trial, _ = parser_release.run_trial("CAND-APP-0001")
        text = trial["格式结果"]["app-text-v1"]
        assert text["样本数"] == 4
        assert text["缺字段率"] == 1.0
        assert text["错列率"] == 1.0
        assert text["超限项"]

    def test_good_candidate_passes_both_formats(self):
        trial, _ = parser_release.run_trial("CAND-APP-0002")
        assert trial["已合格格式"] == ["app-text-v1", "app-json-v2"]
        assert trial["不合格格式"] == []
        assert trial["判定"] == "合格"

    def test_trial_is_recorded_and_reviewable(self):
        trial, _ = parser_release.run_trial("CAND-APP-0001")
        stored = parser_release.find_trial(trial["试解析编号"])
        assert stored is not None
        assert stored["格式结果"]["app-text-v1"]["缺字段率"] == 1.0
        # 样本明细用于事后核对，不会因为换阈值就丢失
        assert stored["样本明细"]["app-text-v1"][0]["解析字段"]

    def test_format_without_samples_is_not_eligible(self):
        candidate, _ = parser_release.create_candidate({
            "来源标识": "app-gateway",
            "规则集名称": "app-gateway-parser",
            "规则集版本": "v9.9",
            "必填字段": ["timestamp"],
            "有意纳入格式": ["app-unknown-v9"],
            "规则定义": {"app-unknown-v9": {"输出字段列表": "timestamp", "解析引擎": "json"}},
        })
        trial, _ = parser_release.run_trial(candidate["候选编号"])
        assert trial["格式结果"]["app-unknown-v9"]["判定"] == "无样本"
        assert trial["格式结果"]["app-unknown-v9"]["样本数"] == 0


class TestPublishGate:
    """发布清单只接受通过质量门槛的格式。"""

    def test_publish_accepts_only_passing_format(self):
        manifest, message = parser_release.publish_candidate("CAND-APP-0001")
        assert manifest["状态"] == "已发布"
        assert manifest["接受格式"] == ["app-json-v2"]
        assert manifest["拒绝格式"] == ["app-text-v1"]
        assert "app-text-v1" not in manifest["已发布规则"]

    def test_manifest_snapshot_is_reviewable(self):
        manifest, _ = parser_release.publish_candidate("CAND-APP-0001")
        verdict = manifest["格式判定"]["app-text-v1"]
        assert verdict["缺字段率"] == 1.0
        assert verdict["字段缺失统计"] == {"host": 4}
        assert manifest["阈值口径"]["app-text-v1"]["缺字段率"]["阈值"] == 0.05
        assert manifest["试解析编号"].startswith("TRIAL-")

    def test_blocked_candidate_never_activates(self):
        # 候选 2 的两种格式里，构造一个全部超限的候选
        candidate, _ = parser_release.create_candidate({
            "来源标识": "app-gateway",
            "规则集名称": "app-gateway-parser",
            "规则集版本": "bad-1",
            "必填字段": ["timestamp", "level", "host", "message"],
            "有意纳入格式": ["app-text-v1"],
            "规则定义": {"app-text-v1": {
                "输出字段列表": "timestamp,level", "解析引擎": "delimited:|"}},
        })
        manifest, message = parser_release.publish_candidate(candidate["候选编号"])
        assert manifest["状态"] == "已阻断"
        assert manifest["当前生效"] is False
        assert "阻断" in message
        assert parser_release.active_manifest("app-gateway") is None

    def test_publish_creates_versioned_rule(self):
        parser_release.publish_candidate("CAND-APP-0001")
        versions = parser_release.list_rule_versions("app-gateway")
        assert len(versions) == 1
        assert versions[0]["规则版本"] == "v2.0/app-json-v2/r1"
        assert versions[0]["状态"] == "已发布"
        assert versions[0]["当前生效"] is True

    def test_republish_keeps_old_rule_for_lookup(self):
        parser_release.publish_candidate("CAND-APP-0001")
        parser_release.publish_candidate("CAND-APP-0002")
        versions = {row["规则版本"]: row for row in parser_release.list_rule_versions("app-gateway")}
        # APP-0001 的 json 规则被替代但记录保留，仍可按原版本回查
        assert "v2.0/app-json-v2/r1" in versions
        assert versions["v2.0/app-json-v2/r1"]["状态"] == "已弃用"
        assert versions["v3.0/app-json-v2/r2"]["状态"] == "已发布"
        # 新版本里旧格式也合格，所以旧格式开始有生产规则
        assert parser_release.eligibility("app-gateway", "app-text-v1")["可生产解析"] is True


class TestInterruptedPublish:
    """发布中断后清单不生效，恢复后从断点继续。"""

    def test_interrupt_keeps_production_on_previous_manifest(self):
        first, _ = parser_release.publish_candidate("CAND-APP-0001")
        assert first["状态"] == "已发布"

        manifest, message = parser_release.publish_candidate(
            "CAND-APP-0002", simulate_interrupt=True)
        assert manifest["状态"] == "发布中断"
        assert manifest["当前生效"] is False
        # 生产仍指向第一份清单
        assert parser_release.active_manifest("app-gateway")["清单编号"] == first["清单编号"]
        # 暂存规则仍是草稿，不可能被生产命中
        staged = [row for row in _rules_with_version() if row.get("发布清单") == manifest["清单编号"]]
        assert staged and all(row["status"] == "草稿" for row in staged)

    def test_resume_does_not_duplicate_rule_versions(self):
        parser_release.publish_candidate("CAND-APP-0001")
        manifest, _ = parser_release.publish_candidate("CAND-APP-0002", simulate_interrupt=True)
        staged_before = {
            (row["输入格式"], row["规则版本"])
            for row in _rules_with_version()
            if row.get("发布清单") == manifest["清单编号"]
        }
        assert staged_before == {("app-text-v1", "v3.0/app-text-v1/r2")}
        resumed, message = parser_release.resume_publish(manifest["清单编号"])
        assert resumed["状态"] == "已发布"
        staged_after = {
            (row["输入格式"], row["规则版本"])
            for row in _rules_with_version()
            if row.get("发布清单") == manifest["清单编号"]
        }
        # 恢复只补齐中断前未暂存的格式，已经暂存的规则不会被重复生成
        assert staged_after == staged_before | {("app-json-v2", "v3.0/app-json-v2/r2")}
        assert parser_release.active_manifest("app-gateway")["清单编号"] == manifest["清单编号"]

    def test_resume_rejects_manifest_that_is_not_interrupted(self):
        manifest, _ = parser_release.publish_candidate("CAND-APP-0001")
        result, message = parser_release.resume_publish(manifest["清单编号"])
        assert result is None
        assert "无需恢复" in message


class TestThresholdRecompute:
    """阈值变更后按新口径重算已试验样本。"""

    def test_threshold_tightening_blocks_published_format(self):
        parser_release.publish_candidate("CAND-APP-0001")
        assert parser_release.eligibility("app-gateway", "app-json-v2")["可生产解析"] is True

        # json 样本的缺字段率是 0；把阈值收紧到一个负值不被允许
        result, _ = parser_release.update_thresholds({
            "来源标识": "app-gateway",
            "输入格式": "app-json-v2",
            "缺字段率": -0.01,
        })
        assert result is None

        # 收紧必填字段口径（加一个样本里不存在的字段）后重算，json 不再合格
        trial = parser_release.find_trial(
            parser_release.active_manifest("app-gateway")["试解析编号"])
        assert trial is not None
        trial["候选快照"]["必填字段"] = ["timestamp", "level", "host", "message", "trace_id"]
        summary = parser_release.recompute_all("必填口径变更")
        assert summary["试解析数"] >= 1
        assert trial["格式结果"]["app-json-v2"]["判定"] == "不合格"
        assert parser_release.eligibility("app-gateway", "app-json-v2")["可生产解析"] is False
        # 清单里也同步记录新口径，便于事后核查
        manifest = parser_release.active_manifest("app-gateway")
        assert manifest["格式判定"]["app-json-v2"]["判定"] == "不合格"
        assert "app-json-v2" in manifest["拒绝格式"]

    def test_loosening_threshold_recovers_published_format(self):
        parser_release.publish_candidate("CAND-APP-0001")
        trial = parser_release.find_trial(
            parser_release.active_manifest("app-gateway")["试解析编号"])
        trial["候选快照"]["必填字段"] = ["timestamp", "level", "host", "message", "trace_id"]
        parser_release.recompute_all("收紧")
        assert parser_release.eligibility("app-gateway", "app-json-v2")["可生产解析"] is False

        trial["候选快照"]["必填字段"] = ["timestamp", "level", "host", "message"]
        parser_release.recompute_all("放宽")
        assert parser_release.eligibility("app-gateway", "app-json-v2")["可生产解析"] is True

    def test_recompute_never_enables_unpublished_format(self):
        parser_release.publish_candidate("CAND-APP-0001")
        # 放宽阈值让 app-text-v1 的试解析合格；但它在发布时被拒绝，没有生产规则，
        # 不应因为阈值放宽自动变成可生产。
        parser_release.update_thresholds({
            "来源标识": "app-gateway",
            "输入格式": "app-text-v1",
            "缺字段率": 1.0,
            "错列率": 1.0,
        })
        trial = parser_release.find_trial(
            parser_release.active_manifest("app-gateway")["试解析编号"])
        assert "app-text-v1" in trial["已合格格式"]
        assert parser_release.eligibility("app-gateway", "app-text-v1")["可生产解析"] is False

    def test_threshold_change_only_applies_to_named_format(self):
        parser_release.update_thresholds({
            "来源标识": "app-gateway",
            "输入格式": "app-text-v1",
            "缺字段率": 0.01,
        })
        missing_text = parser_release.resolve_threshold(
            parser_release.MISSING_METRIC, "app-gateway", "app-text-v1")
        missing_json = parser_release.resolve_threshold(
            parser_release.MISSING_METRIC, "app-gateway", "app-json-v2")
        assert missing_text["阈值"] == 0.01
        assert missing_json["阈值"] == 0.05
        # 错列率没有被重复设置，仍旧使用来源级阈值
        misaligned_text = parser_release.resolve_threshold(
            parser_release.MISALIGNED_METRIC, "app-gateway", "app-text-v1")
        assert misaligned_text["阈值"] == 0.02


class TestProductionParsing:
    """生产解析只使用发布清单里合格的格式和固定规则版本。"""

    def _ingest(self, pipeline, fmt, logs):
        batch = pipeline.start_ingest_batch(
            "app-gateway", f"2026-01-08T{fmt}", logs, default_format=fmt)
        assert batch["ok"] is True
        return batch["batch"]["批次编号"]

    def _close_batch(self, batch_no):
        _batch(batch_no)["status"] = "已完成"

    def test_eligible_format_parses_with_manifest_rule_version(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": json.dumps({"ts": "2026-01-08T11:00:00", "lvl": "INFO",
                                 "host": "app-gateway-03", "msg": "ok"})},
        ])
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 1
        entity = [e for e in store.rows("parsed_entity") if e.get("来源批次") == batch_no][0]
        assert entity["解析规则版本"] == "v2.0/app-json-v2/r1"
        assert json.loads(entity["标准化字段集"]) == {
            "timestamp": "2026-01-08T11:00:00",
            "level": "INFO",
            "host": "app-gateway-03",
            "message": "ok",
        }

    def test_ineligible_format_is_blocked_with_reason(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = self._ingest(pipeline, "app-text-v1", [
            {"body": "2026-01-08T10:00:00|INFO|app-gateway-01|ok"},
        ])
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 0
        assert result["failure"] == 1
        assert "app-text-v1" in result["blocked_records"][0]["原因"]
        retry = [r for r in store.rows("retry_queue") if r.get("batch_no") == batch_no][0]
        assert "质量门槛" in retry["失败原因"]

    def test_batch_pins_mapping_across_later_publish(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"})},
            {"body": json.dumps({"ts": "t2", "lvl": "INFO", "host": "h1", "msg": "b"})},
        ])
        # 批次启动后，并发发布新清单
        parser_release.publish_candidate("CAND-APP-0002")
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 2
        versions = {
            e["解析规则版本"]
            for e in store.rows("parsed_entity")
            if e.get("来源批次") == batch_no
        }
        assert versions == {"v2.0/app-json-v2/r1"}
        assert result["manifest"] == "MAN-0001"

    def test_mixed_formats_in_one_batch_use_independent_rules(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"}),
             "format": "app-json-v2"},
            {"body": "2026-01-08T10:00:00|INFO|app-gateway-01|ok", "format": "app-text-v1"},
        ])
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 1
        assert result["failure"] == 1
        assert set(result["format_rules"]) == {"app-json-v2"}

    def test_gated_source_without_manifest_blocks_all_formats(self, pipeline):
        # app-gateway 已登记样本/候选，属于门槛管理的来源；没有生效清单时
        # 不能借用任何已发布规则兜底。
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": "t1|INFO|msg"},
        ])
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 0
        assert result["failure"] == 1
        assert "还没有生效的解析发布清单" in result["blocked_records"][0]["原因"]

    def test_ungated_source_falls_back_to_legacy_rule(self, pipeline):
        # 完全没有门槛配置的历史来源保持原行为，避免破坏既有调用。
        pipeline.register_source("legacy-src", "http", "default")
        pipeline.validate_source("legacy-src")
        batch_no = pipeline.start_ingest_batch(
            "legacy-src", "2026-01-08T16",
            [{"body": "t1|INFO|msg"}], default_format="legacy-fmt",
        )["batch"]["批次编号"]
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 1

    def test_batch_started_without_manifest_does_not_pick_one_up_mid_parse(self, pipeline):
        # 批次启动时还没有清单；解析前清单已发布完成，但本批次不应换映射。
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"})},
        ])
        parser_release.publish_candidate("CAND-APP-0001")
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 0
        assert result["manifest"] is None
        assert "还没有生效的解析发布清单" in result["blocked_records"][0]["原因"]

    def test_draft_rule_cannot_be_used_explicitly(self, pipeline):
        manifest, _ = parser_release.publish_candidate(
            "CAND-APP-0001", simulate_interrupt=True)
        staged = [row for row in _rules_with_version()
                  if row.get("发布清单") == manifest["清单编号"]][0]
        batch_no = self._ingest(pipeline, "app-json-v2", [
            {"body": json.dumps({"ts": "t", "lvl": "INFO", "host": "h", "msg": "m"})},
        ])
        result = pipeline.parse_batch(batch_no, staged["规则名称"])
        assert result["ok"] is False
        assert "尚未发布" in result["message"]


class TestVersionedLookup:
    """已完成的日志仍可按原规则版本回查。"""

    def test_records_lookup_by_rule_version(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = pipeline.start_ingest_batch(
            "app-gateway", "2026-01-08T12",
            [{"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"}),
              "format": "app-json-v2"}],
        )["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        lookup = parser_release.records_by_rule_version("v2.0/app-json-v2/r1")
        assert lookup["记录数"] == 1
        assert lookup["规则"]["输入格式"] == "app-json-v2"
        assert lookup["规则"]["字段映射"] == {"timestamp": "ts", "level": "lvl", "message": "msg"}

    def test_source_stats_explain_rule_sets(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        batch_no = pipeline.start_ingest_batch(
            "app-gateway", "2026-01-08T13",
            [{"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"}),
              "format": "app-json-v2"}],
        )["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        stats = parser_release.source_stats("app-gateway")
        assert stats["记录数"] == 1
        assert stats["使用的规则集"][0]["规则集版本"] == "v2.0"
        assert "规则集说明" in stats
        assert stats["当前生效规则集"]["清单编号"] == "MAN-0001"

    def test_source_stats_counts_both_formats_with_their_rule_sets(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")

        first = pipeline.start_ingest_batch(
            "app-gateway", "2026-01-08T14",
            [{"body": json.dumps({"ts": "t1", "lvl": "INFO", "host": "h1", "msg": "a"}),
              "format": "app-json-v2"}],
        )["batch"]["批次编号"]
        pipeline.parse_batch(first)
        _batch(first)["status"] = "已完成"

        # 旧格式在 v2.0 清单里被阻断；v3.0 清单发布后旧格式才具备资格
        parser_release.publish_candidate("CAND-APP-0002")

        second = pipeline.start_ingest_batch(
            "app-gateway", "2026-01-08T15",
            [{"body": "2026-01-08T15:00:00|INFO|app-gateway-01|ok", "format": "app-text-v1"}],
        )["batch"]["批次编号"]
        pipeline.parse_batch(second)

        stats = parser_release.source_stats("app-gateway")
        assert stats["记录数"] == 2
        assert stats["格式数"] == 2
        versions = {(item["输入格式"], item["规则集版本"]) for item in stats["使用的规则集"]}
        assert versions == {("app-json-v2", "v2.0"), ("app-text-v1", "v3.0")}
        assert stats["当前生效规则集"]["规则集版本"] == "v3.0"
        # 同源合计说明了用了哪几套规则、每套覆盖什么格式
        assert "v3.0" in stats["规则集说明"]  # 当前规则集在说明里点名
        assert "共使用 2 套规则版本" in stats["规则集说明"]


class TestSupportScope:
    """支持范围按格式独立核对，超限格式不进入生产。"""

    def test_support_scope_lists_each_format_verdict(self, pipeline):
        parser_release.publish_candidate("CAND-APP-0001")
        manifest = parser_release.active_manifest("app-gateway")
        assert manifest["有意纳入格式"] == ["app-text-v1", "app-json-v2"]
        assert manifest["接受格式"] == ["app-json-v2"]
        text = parser_release.eligibility("app-gateway", "app-text-v1")
        assert text["可生产解析"] is False
        assert text["判定"] == "不合格"
        assert text["超限项"]
        json_scope = parser_release.eligibility("app-gateway", "app-json-v2")
        assert json_scope["可生产解析"] is True
        assert json_scope["规则"]["规则版本"] == "v2.0/app-json-v2/r1"

    def test_source_without_manifest_cannot_produce(self):
        scope = parser_release.eligibility("app-gateway", "app-json-v2")
        assert scope["可生产解析"] is False
        assert scope["清单编号"] is None

    def test_candidate_can_only_cover_its_source(self):
        candidate, _ = parser_release.create_candidate({
            "来源标识": "other-source",
            "规则集名称": "other-parser",
            "规则集版本": "v1",
            "必填字段": ["host"],
            "有意纳入格式": ["app-json-v2"],
            "规则定义": {"app-json-v2": {"输出字段列表": "host", "解析引擎": "json"}},
        })
        trial, _ = parser_release.run_trial(candidate["候选编号"])
        # 别的来源没有该格式样本，不能借用 app-gateway 的样本
        assert trial["格式结果"]["app-json-v2"]["样本数"] == 0
        assert trial["格式结果"]["app-json-v2"]["判定"] == "无样本"
