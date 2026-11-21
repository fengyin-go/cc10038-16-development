<template>
  <section class="page parser-release">
    <header class="page-head">
      <div>
        <h2>解析发布门槛</h2>
        <p class="page-desc">
          按来源格式试解析样本，用质量规则判定候选能否发布；发布清单记录每种格式的生产解析资格。
        </p>
      </div>
      <div class="page-actions">
        <label class="source-picker">
          <span>来源</span>
          <select v-model="sourceId" data-testid="source-select" @change="reloadAll">
            <option value="app-gateway">app-gateway</option>
          </select>
        </label>
        <button class="btn" data-testid="refresh" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</p>

    <section class="panel">
      <header class="panel-head">
        <h3>候选规则与试解析</h3>
        <span class="panel-hint">每个有意纳入的格式按其阈值独立判定</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>候选</th>
            <th>规则集</th>
            <th>有意纳入格式</th>
            <th>必填字段</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="candidate in candidates" :key="candidate.候选编号" :data-testid="`candidate-${candidate.候选编号}`">
            <td>{{ candidate.候选编号 }}</td>
            <td>{{ candidate.规则集名称 }}@{{ candidate.规则集版本 }}</td>
            <td>{{ candidate.有意纳入格式.join('、') }}</td>
            <td>{{ candidate.必填字段.join('、') }}</td>
            <td>{{ candidate.状态 }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`trial-${candidate.候选编号}`" @click="doTrial(candidate.候选编号)">
                试解析
              </button>
              <button class="link" type="button" :data-testid="`publish-${candidate.候选编号}`" @click="doPublish(candidate.候选编号, false)">
                发布
              </button>
              <button class="link" type="button" :data-testid="`publish-interrupt-${candidate.候选编号}`" @click="doPublish(candidate.候选编号, true)">
                模拟中断发布
              </button>
            </td>
          </tr>
          <tr v-if="!candidates.length">
            <td colspan="6" class="empty-state">当前来源没有候选规则</td>
          </tr>
        </tbody>
      </table>

      <div v-if="activeTrial" class="trial-result" data-testid="trial-result">
        <h4>试解析 {{ activeTrial.试解析编号 }} · 判定：{{ activeTrial.判定 }}</h4>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>输入格式</th>
              <th>样本数</th>
              <th>缺字段率</th>
              <th>缺字段阈值</th>
              <th>错列率</th>
              <th>错列阈值</th>
              <th>判定</th>
              <th>超限项</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(verdict, fmt) in activeTrial.格式结果" :key="fmt" :data-testid="`trial-format-${fmt}`">
              <td>{{ fmt }}</td>
              <td>{{ verdict.样本数 }}</td>
              <td>{{ percent(verdict.缺字段率) }}</td>
              <td>{{ percent(verdict.缺字段阈值) }}</td>
              <td>{{ percent(verdict.错列率) }}</td>
              <td>{{ percent(verdict.错列阈值) }}</td>
              <td>
                <span class="badge" :class="verdict.判定 === '合格' ? 'ok' : 'bad'">{{ verdict.判定 }}</span>
              </td>
              <td class="limit-cell">
                <span v-for="item in verdict.超限项" :key="item.指标">
                  {{ item.指标 }}：{{ item.实际 }} / {{ item.阈值 }}（{{ item.说明 }}）
                </span>
                <span v-if="!verdict.超限项.length">—</span>
              </td>
            </tr>
          </tbody>
        </table>
        <p v-for="(verdict, fmt) in activeTrial.格式结果" :key="`detail-${fmt}`" class="field-detail">
          <strong>{{ fmt }}</strong>
          缺字段：{{ formatStats(verdict.字段缺失统计) }}；
          错列：{{ formatStats(verdict.错列字段统计) }}
        </p>
      </div>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>发布清单</h3>
        <span class="panel-hint">清单记录每种格式的资格；中断时清单不生效</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>清单</th>
            <th>规则集</th>
            <th>试解析</th>
            <th>接受格式</th>
            <th>拒绝格式</th>
            <th>状态</th>
            <th>生效</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="manifest in manifests" :key="manifest.清单编号" :data-testid="`manifest-${manifest.清单编号}`">
            <td>{{ manifest.清单编号 }}</td>
            <td>{{ manifest.规则集名称 }}@{{ manifest.规则集版本 }}</td>
            <td>{{ manifest.试解析编号 }}</td>
            <td>
              <span v-for="fmt in manifest.接受格式" :key="fmt" class="tag ok">{{ fmt }}</span>
              <span v-if="!manifest.接受格式.length">—</span>
            </td>
            <td>
              <span v-for="fmt in manifest.拒绝格式" :key="fmt" class="tag bad">{{ fmt }}</span>
              <span v-if="!manifest.拒绝格式.length">—</span>
            </td>
            <td>{{ manifest.状态 }}</td>
            <td>{{ manifest.当前生效 ? '是' : '否' }}</td>
            <td class="row-actions">
              <button
                v-if="manifest.状态 === '发布中断'"
                class="link"
                type="button"
                :data-testid="`resume-${manifest.清单编号}`"
                @click="doResume(manifest.清单编号)"
              >
                恢复发布
              </button>
              <button class="link" type="button" :data-testid="`inspect-${manifest.清单编号}`" @click="inspectManifest(manifest)">
                查看质量结果
              </button>
            </td>
          </tr>
          <tr v-if="!manifests.length">
            <td colspan="8" class="empty-state">尚无发布清单</td>
          </tr>
        </tbody>
      </table>
      <p v-if="inspected" class="manifest-detail" data-testid="manifest-detail">
        {{ inspected.清单编号 }}：
        <span v-for="(verdict, fmt) in inspected.格式判定" :key="fmt">
          {{ fmt }} 缺字段 {{ percent(verdict.缺字段率) }}（阈值 {{ percent(verdict.缺字段阈值) }}）、
          错列 {{ percent(verdict.错列率) }}（阈值 {{ percent(verdict.错列阈值) }}），判定 {{ verdict.判定 }}；
        </span>
      </p>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>支持范围与质量结果</h3>
        <span class="panel-hint">每个格式按其判定条件独立核对，超限样本对应规则不得用于该类生产日志</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>输入格式</th>
            <th>样本数</th>
            <th>缺字段阈值</th>
            <th>错列阈值</th>
            <th>可生产解析</th>
            <th>依据</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in scope" :key="item.输入格式" :data-testid="`scope-${item.输入格式}`">
            <td>{{ item.输入格式 }}</td>
            <td>{{ item.样本数 }}</td>
            <td>{{ percent(item.判定条件.缺字段阈值.阈值) }}（{{ item.判定条件.缺字段阈值.规则名称 }}）</td>
            <td>{{ percent(item.判定条件.错列阈值.阈值) }}（{{ item.判定条件.错列阈值.规则名称 }}）</td>
            <td>
              <span class="badge" :class="item.资格.可生产解析 ? 'ok' : 'bad'">
                {{ item.资格.可生产解析 ? '可用' : '不可用' }}
              </span>
            </td>
            <td class="limit-cell">
              <span>{{ item.资格.原因 }}</span>
              <span v-if="item.资格.规则">；规则版本 {{ item.资格.规则.规则版本 }}</span>
              <span v-if="item.资格.判定 && !item.资格.可生产解析">；判定 {{ item.资格.判定 }}</span>
            </td>
          </tr>
          <tr v-if="!scope.length">
            <td colspan="6" class="empty-state">没有可用的格式样本</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>同源合计</h3>
        <span class="panel-hint">两类日志都进来后，说明合计用了哪几套规则</span>
      </header>
      <p class="stat-line" data-testid="source-summary">{{ stats?.规则集说明 || '暂无记录' }}</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>输入格式</th>
            <th>记录数</th>
            <th>规则版本使用</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(bucket, fmt) in stats?.格式统计 || {}" :key="fmt" :data-testid="`stats-${fmt}`">
            <td>{{ fmt }}</td>
            <td>{{ bucket.记录数 }}</td>
            <td>
              <span v-for="(count, version) in bucket.规则版本使用" :key="version" class="tag neutral">
                {{ version }} × {{ count }}
              </span>
            </td>
          </tr>
          <tr v-if="!stats || !stats.记录数">
            <td colspan="3" class="empty-state">当前来源还没有标准化记录</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>阈值调整</h3>
        <span class="panel-hint">阈值变更后按新口径重算已试验的样本</span>
      </header>
      <form class="threshold-form" @submit.prevent="doUpdateThresholds">
        <label>
          <span>输入格式（留空为来源级）</span>
          <input v-model="thresholdForm.输入格式" data-testid="threshold-format" placeholder="app-text-v1" />
        </label>
        <label>
          <span>缺字段率</span>
          <input v-model="thresholdForm.缺字段率" data-testid="threshold-missing" placeholder="0.05" />
        </label>
        <label>
          <span>错列率</span>
          <input v-model="thresholdForm.错列率" data-testid="threshold-misaligned" placeholder="0.02" />
        </label>
        <button class="btn primary" data-testid="apply-threshold" type="submit">应用并重算</button>
      </form>
      <p v-if="recomputeSummary" class="stat-line" data-testid="recompute-summary">{{ recomputeSummary }}</p>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>版本回查</h3>
        <span class="panel-hint">已完成的日志仍可按原规则版本查看</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>规则版本</th>
            <th>输入格式</th>
            <th>规则集</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="version in ruleVersions" :key="version.规则版本" :data-testid="`version-${version.规则版本}`">
            <td>{{ version.规则版本 }}</td>
            <td>{{ version.输入格式 }}</td>
            <td>{{ version.规则集名称 }}@{{ version.规则集版本 }}</td>
            <td>
              <span class="tag" :class="version.当前生效 ? 'ok' : 'neutral'">{{ version.状态 }}</span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`lookup-${version.规则版本}`" @click="lookup(version.规则版本)">
                查看记录
              </button>
            </td>
          </tr>
          <tr v-if="!ruleVersions.length">
            <td colspan="5" class="empty-state">尚无版本化规则</td>
          </tr>
        </tbody>
      </table>
      <p v-if="lookupResult" class="stat-line" data-testid="lookup-result">
        {{ lookupResult.ruleVersion }} 共 {{ lookupResult.count }} 条标准化记录
      </p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  getFormatScope,
  getSourceStats,
  listCandidates,
  listManifests,
  listRuleVersions,
  publishCandidate,
  recordsByRuleVersion,
  resumePublish,
  runTrial,
  updateThresholds,
  type Candidate,
  type FormatScope,
  type Manifest,
  type SourceStats,
  type Trial,
} from '@/api/parser-release'

const sourceId = ref('app-gateway')
const errorMessage = ref('')
const notice = ref('')
const candidates = ref<Candidate[]>([])
const manifests = ref<Manifest[]>([])
const scope = ref<FormatScope[]>([])
const stats = ref<SourceStats | null>(null)
const activeTrial = ref<Trial | null>(null)
const inspected = ref<Manifest | null>(null)
const ruleVersions = ref<Awaited<ReturnType<typeof listRuleVersions>>['items']>([])
const lookupResult = ref<{ ruleVersion: string; count: number } | null>(null)
const thresholdForm = ref({ 输入格式: '', 缺字段率: '', 错列率: '' })
const recomputeSummary = ref('')

function percent(value: number): string {
  return `${(value * 100).toFixed(1)}%`
}

function formatStats(statsMap: Record<string, number> | undefined): string {
  const entries = Object.entries(statsMap || {})
  if (!entries.length) return '无'
  return entries.map(([field, count]) => `${field}×${count}`).join('、')
}

function clearMessages() {
  errorMessage.value = ''
  notice.value = ''
}

async function reloadAll() {
  clearMessages()
  try {
    const [candidateRows, manifestRows, scopePayload, statsPayload] = await Promise.all([
      listCandidates(sourceId.value),
      listManifests(sourceId.value),
      getFormatScope(sourceId.value),
      getSourceStats(sourceId.value),
    ])
    candidates.value = candidateRows
    manifests.value = manifestRows
    scope.value = scopePayload.格式范围
    stats.value = statsPayload
    const versions = await listRuleVersions(sourceId.value)
    ruleVersions.value = versions.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据读取失败'
  }
}

async function doTrial(candidateId: string) {
  clearMessages()
  try {
    activeTrial.value = await runTrial(candidateId)
    notice.value = `试解析完成：${activeTrial.value.判定}`
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '试解析失败'
  }
}

async function doPublish(candidateId: string, simulateInterrupt: boolean) {
  clearMessages()
  try {
    const result = await publishCandidate(candidateId, simulateInterrupt)
    notice.value = result.message
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布失败'
  }
}

async function doResume(manifestId: string) {
  clearMessages()
  try {
    const result = await resumePublish(manifestId)
    notice.value = result.message
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复发布失败'
  }
}

function inspectManifest(manifest: Manifest) {
  inspected.value = manifest
}

async function doUpdateThresholds() {
  clearMessages()
  const values: Record<string, unknown> = { 来源标识: sourceId.value }
  if (thresholdForm.value.输入格式.trim()) {
    values.输入格式 = thresholdForm.value.输入格式.trim()
  }
  if (thresholdForm.value.缺字段率.trim()) {
    values.缺字段率 = Number(thresholdForm.value.缺字段率)
  }
  if (thresholdForm.value.错列率.trim()) {
    values.错列率 = Number(thresholdForm.value.错列率)
  }
  try {
    const result = await updateThresholds(values)
    const changes = result.重算?.判定变化?.length ?? 0
    const manifestChanges = result.重算?.清单变化?.length ?? 0
    recomputeSummary.value = `已重算 ${result.重算 ? '已试验样本' : ''}：判定变化 ${changes} 条，清单变化 ${manifestChanges} 条`
    notice.value = result.message
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '阈值更新失败'
  }
}

async function lookup(ruleVersion: string) {
  clearMessages()
  try {
    const result = await recordsByRuleVersion(ruleVersion)
    lookupResult.value = { ruleVersion, count: result.记录数 }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '版本回查失败'
  }
}

onMounted(reloadAll)
</script>
