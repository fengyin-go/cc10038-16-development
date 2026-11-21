/** 解析发布门槛接口：样本、候选试解析、发布清单、阈值重算、版本回查。 */
import { request } from './client'

export type FormatVerdict = {
  输入格式: string
  规则名称?: string
  样本数: number
  缺字段率: number
  错列率: number
  缺字段阈值: number
  错列阈值: number
  判定: string
  超限项: { 指标: string; 实际: number; 阈值: number | null; 说明: string }[]
  字段缺失统计: Record<string, number>
  错列字段统计: Record<string, number>
  值不符统计?: Record<string, number>
}

export type Trial = {
  id: number
  试解析编号: string
  候选编号: string
  来源标识: string
  规则集名称: string
  规则集版本: string
  格式结果: Record<string, FormatVerdict>
  已合格格式: string[]
  不合格格式: string[]
  判定: string
  试解析时间: string
  最近重算时间?: string
}

export type Manifest = {
  id: number
  清单编号: string
  来源标识: string
  候选编号: string
  规则集名称: string
  规则集版本: string
  试解析编号: string
  有意纳入格式: string[]
  接受格式: string[]
  拒绝格式: string[]
  已发布格式: string[]
  已暂存格式: string[]
  格式判定: Record<string, FormatVerdict>
  已发布规则: Record<string, { 规则名称: string; 规则版本: string; 规则集版本: string }>
  状态: string
  当前生效: boolean
  创建时间: string
  发布时间?: string
  中断位置?: string
  阻断原因?: string
}

export type Candidate = {
  id: number
  候选编号: string
  来源标识: string
  规则集名称: string
  规则集版本: string
  必填字段: string[]
  有意纳入格式: string[]
  规则定义: Record<string, { 规则名称: string; 输出字段列表: string; 解析引擎: string }>
  状态: string
  说明?: string
}

export type Eligibility = {
  来源标识: string
  输入格式: string
  可生产解析: boolean
  原因: string
  清单编号: string | null
  判定?: string
  超限项?: FormatVerdict['超限项']
  字段缺失统计?: Record<string, number>
  错列字段统计?: Record<string, number>
  规则?: { 规则名称: string; 规则版本: string; 规则集版本: string }
}

export type FormatScope = {
  输入格式: string
  样本数: number
  资格: Eligibility
  判定条件: Record<string, { 阈值: number; 规则名称: string; 来源: string }>
}

export type SourceStats = {
  来源标识: string
  记录数: number
  格式数: number
  格式统计: Record<string, {
    输入格式: string
    记录数: number
    规则版本使用: Record<string, number>
    校验状态: Record<string, number>
  }>
  使用的规则集: {
    规则集名称: string
    规则集版本: string
    输入格式: string
    规则版本: string
    记录数: number
  }[]
  当前生效规则集: {
    清单编号: string
    规则集名称: string
    规则集版本: string
    接受格式: string[]
  } | null
  规则集说明: string
  可生产格式: string[]
  阻断格式: string[]
}

async function json<T>(response: Response, fallback: string): Promise<T> {
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = payload && typeof payload.detail === 'string' ? payload.detail : fallback
    throw new Error(detail)
  }
  return payload as T
}

export async function listCandidates(sourceId?: string): Promise<Candidate[]> {
  const query = sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : ''
  const response = await request(`/api/parser/candidates${query}`)
  const payload = await json<{ items: Candidate[] }>(response, '候选列表读取失败')
  return payload.items
}

export async function runTrial(candidateId: string): Promise<Trial> {
  const response = await request(`/api/parser/candidates/${encodeURIComponent(candidateId)}/trial`, {
    method: 'POST',
  })
  const payload = await json<{ trial: Trial }>(response, '试解析失败')
  return payload.trial
}

export async function publishCandidate(
  candidateId: string,
  simulateInterrupt = false,
): Promise<{ message: string; manifest: Manifest }> {
  const suffix = simulateInterrupt ? '?simulate_interrupt=true' : ''
  const response = await request(
    `/api/parser/candidates/${encodeURIComponent(candidateId)}/publish${suffix}`,
    { method: 'POST' },
  )
  return await json<{ message: string; manifest: Manifest }>(response, '发布失败')
}

export async function resumePublish(manifestId: string): Promise<{ message: string; manifest: Manifest }> {
  const response = await request(`/api/parser/manifests/${encodeURIComponent(manifestId)}/resume`, {
    method: 'POST',
  })
  return await json<{ message: string; manifest: Manifest }>(response, '恢复发布失败')
}

export async function listManifests(sourceId?: string): Promise<Manifest[]> {
  const query = sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : ''
  const response = await request(`/api/parser/manifests${query}`)
  const payload = await json<{ items: Manifest[] }>(response, '发布清单读取失败')
  return payload.items
}

export async function getFormatScope(sourceId: string): Promise<{ 格式范围: FormatScope[]; 生效清单: Manifest | null }> {
  const response = await request(`/api/parser/sources/${encodeURIComponent(sourceId)}/formats`)
  return await json(response, '支持范围读取失败')
}

export async function getSourceStats(sourceId: string): Promise<SourceStats> {
  const response = await request(`/api/parser/sources/${encodeURIComponent(sourceId)}/stats`)
  return await json(response, '同源合计读取失败')
}

export async function updateThresholds(values: Record<string, unknown>): Promise<{ message: string; 重算: { 判定变化: unknown[]; 清单变化: unknown[] } }> {
  const response = await request('/api/parser/thresholds', {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await json(response, '阈值更新失败')
}

export async function listRuleVersions(sourceId?: string): Promise<{
  items: { 规则名称: string; 规则版本: string; 输入格式: string; 规则集名称: string; 规则集版本: string; 状态: string; 当前生效: boolean }[]
}> {
  const query = sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : ''
  const response = await request(`/api/parser/rule-versions${query}`)
  return await json(response, '规则版本读取失败')
}

export async function recordsByRuleVersion(ruleVersion: string): Promise<{ 记录数: number; 记录: Record<string, unknown>[] }> {
  const response = await request(`/api/parser/records?rule_version=${encodeURIComponent(ruleVersion)}`)
  return await json(response, '版本回查失败')
}
