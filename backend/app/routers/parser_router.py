"""解析发布门槛接口：样本、候选试解析、发布清单、阈值重算与版本回查。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import parser_release

router = APIRouter(prefix="/api/parser", tags=["解析发布门槛"])


class SamplePayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class CandidatePayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class ThresholdPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


@router.get("/samples")
def list_samples(
    source_id: str | None = Query(default=None),
    format: str | None = Query(default=None),
) -> dict[str, Any]:
    rows = parser_release.list_samples(source_id, format)
    return {"items": rows, "total": len(rows)}


@router.post("/samples")
def create_sample(payload: SamplePayload) -> dict[str, Any]:
    row, missing = parser_release.create_sample(payload.values)
    if missing:
        return {"ok": False, "message": f"缺少必填字段：{'、'.join(missing)}"}
    return {"ok": True, "message": "样本已登记", "entry": row}


@router.get("/candidates")
def list_candidates(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = parser_release.list_candidates(source_id)
    return {"items": rows, "total": len(rows)}


@router.post("/candidates")
def create_candidate(payload: CandidatePayload) -> dict[str, Any]:
    row, missing = parser_release.create_candidate(payload.values)
    if missing:
        return {"ok": False, "message": f"缺少必填字段：{'、'.join(missing)}"}
    return {"ok": True, "message": "候选已登记", "entry": row}


@router.post("/candidates/{candidate_id}/trial")
def run_trial(candidate_id: str) -> dict[str, Any]:
    trial, error = parser_release.run_trial(candidate_id)
    if trial is None:
        raise HTTPException(status_code=404, detail=error)
    return {"ok": True, "message": "试解析完成", "trial": trial}


@router.get("/trials")
def list_trials(candidate_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = parser_release.list_trials(candidate_id)
    return {"items": rows, "total": len(rows)}


@router.get("/trials/{trial_id}")
def get_trial(trial_id: str) -> dict[str, Any]:
    trial = parser_release.find_trial(trial_id)
    if trial is None:
        raise HTTPException(status_code=404, detail=f"试解析 {trial_id} 不存在")
    return trial


@router.post("/candidates/{candidate_id}/publish")
def publish_candidate(
    candidate_id: str,
    simulate_interrupt: bool = Query(default=False),
) -> dict[str, Any]:
    manifest, message = parser_release.publish_candidate(
        candidate_id, simulate_interrupt=simulate_interrupt)
    if manifest is None:
        raise HTTPException(status_code=400, detail=message)
    return {
        "ok": manifest.get("状态") in ("已发布", "发布中断"),
        "message": message,
        "manifest": manifest,
    }


@router.post("/manifests/{manifest_id}/resume")
def resume_publish(manifest_id: str) -> dict[str, Any]:
    manifest, message = parser_release.resume_publish(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "manifest": manifest}


@router.get("/manifests")
def list_manifests(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = parser_release.list_manifests(source_id)
    return {"items": rows, "total": len(rows)}


@router.get("/manifests/{manifest_id}")
def get_manifest(manifest_id: str) -> dict[str, Any]:
    for row in parser_release.list_manifests():
        if str(row.get("id")) == manifest_id or row.get("清单编号") == manifest_id:
            return row
    raise HTTPException(status_code=404, detail=f"发布清单 {manifest_id} 不存在")


@router.get("/eligibility/{source_id}")
def get_eligibility(
    source_id: str,
    format: str | None = Query(default=None),
) -> dict[str, Any]:
    if format:
        return parser_release.eligibility(source_id, format)
    return {
        "来源标识": source_id,
        "格式资格": parser_release.eligibility_map(source_id),
        "生效清单": parser_release.active_manifest(source_id),
    }


@router.get("/sources/{source_id}/formats")
def support_scope(source_id: str) -> dict[str, Any]:
    """支持范围：哪些格式可用、各自的判定条件与当前生效清单。"""
    manifest = parser_release.active_manifest(source_id)
    samples = parser_release.list_samples(source_id)
    formats = sorted({row.get("输入格式") for row in samples if row.get("输入格式")})
    if manifest:
        formats = sorted(set(formats) | set(manifest.get("有意纳入格式") or []))
    scope = []
    for fmt in formats:
        entries = [row for row in samples if row.get("输入格式") == fmt]
        scope.append({
            "输入格式": fmt,
            "样本数": len(entries),
            "资格": parser_release.eligibility(source_id, fmt),
            "判定条件": {
                "缺字段阈值": parser_release.resolve_threshold(
                    parser_release.MISSING_METRIC, source_id, fmt),
                "错列阈值": parser_release.resolve_threshold(
                    parser_release.MISALIGNED_METRIC, source_id, fmt),
            },
        })
    return {
        "来源标识": source_id,
        "格式范围": scope,
        "生效清单": manifest,
        "按格式独立判定": True,
    }


@router.get("/sources/{source_id}/stats")
def source_stats(source_id: str) -> dict[str, Any]:
    return parser_release.source_stats(source_id)


@router.post("/thresholds")
def update_thresholds(payload: ThresholdPayload) -> dict[str, Any]:
    result, message = parser_release.update_thresholds(payload.values)
    if result is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, **result}


@router.post("/recompute")
def recompute() -> dict[str, Any]:
    return {"ok": True, "message": "已按当前口径重算", "summary": parser_release.recompute_all()}


@router.get("/rule-versions")
def list_rule_versions(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = parser_release.list_rule_versions(source_id)
    return {"items": rows, "total": len(rows)}


@router.get("/records")
def records_by_rule_version(
    rule_version: str = Query(..., min_length=1),
    source_id: str | None = Query(default=None),
    format: str | None = Query(default=None),
) -> dict[str, Any]:
    return parser_release.records_by_rule_version(rule_version, source_id, format)
