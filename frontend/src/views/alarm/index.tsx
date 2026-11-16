import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/alarm'
const columns = [
  "规则编号",
  "监控指标",
  "告警条件",
  "告警级别",
  "通知渠道",
  "静默周期",
  "升级策略",
  "规则状态"
]
const actions = [
  "启用告警",
  "暂停告警",
  "关闭告警"
]
const statuses = [
  "草稿",
  "已启用",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "告警规则总数",
    "value": 0
  },
  {
    "label": "已启用告警",
    "value": 0
  },
  {
    "label": "已暂停告警",
    "value": 0
  }
]

type Row = Record<string, string | number | null>
type Decision = Record<string, unknown>

const decisionColumns = [
  '判定编号',
  '规则编号',
  '指标版本',
  '复核结论',
  '复核状态',
  '通知事件',
  '上一条有效结果',
]

function previousVersion(value: unknown): string {
  if (!value || typeof value !== 'object') {
    return '—'
  }
  const previous = value as Record<string, unknown>
  return String(previous['指标版本'] ?? previous.metric_version ?? '—')
}

function eventText(value: unknown): string {
  return Array.isArray(value) && value.length > 0 ? value.join('、') : '—'
}

export default function AlarmView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [decisions, setDecisions] = useState<Decision[]>([])
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('告警规则列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      const decisionResponse = await request(`${ENDPOINT}/decisions?size=50`)
      if (decisionResponse.ok) {
        const decisionPayload = await decisionResponse.json()
        setDecisions(decisionPayload.items ?? [])
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流告警规则列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function post(path: string, body: Record<string, unknown>) {
    setErrorMessage('')
    try {
      const response = await request(path, {
        method: 'POST',
        body: JSON.stringify({ values: body }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok) {
        throw new Error(payload?.detail ?? '请求未生效，请稍后重试')
      }
      if (payload?.message) {
        setErrorMessage(payload.message)
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流告警规则操作失败')
    }
  }

  async function runAction(action: string, row: Row) {
    await post(`${ENDPOINT}/${row.id}/actions`, { action })
  }

  async function recompute(row: Row) {
    const batch = window.prompt('请输入指标批次编号', `batch-${row.id}`)
    if (!batch) {
      return
    }
    const sample = window.prompt('请输入该批次的 avg-sample 采样值', '860')
    if (sample === null) {
      return
    }
    await post(`${ENDPOINT}/decisions/recompute`, {
      规则编号: row['规则编号'],
      批次编号: batch,
      'avg-sample': Number(sample),
    })
  }

  return (
    <section className="page" data-module="alarm">
      <header className="page-head">
        <div>
          <h2>流告警规则管理</h2>
          <p className="page-desc">维护流告警的监控指标、触发条件与通知策略，为流告警规则管理提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('告警规则登记入口尚未接入审批流')}>
            登记告警规则
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出流告警规则清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
                <button className="link" type="button" onClick={() => void recompute(row)}>
                  重算判定
                </button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无流告警规则数据，可先登记告警规则</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">告警判定数据结果</h3>
      <table className="data-table">
        <thead>
          <tr>
            {decisionColumns.map((column) => <th key={column}>{column}</th>)}
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {decisions.map((decision) => (
            <tr key={String(decision['判定编号'])}>
              <td>{String(decision['判定编号'] ?? '—')}</td>
              <td>{String(decision['规则编号'] ?? '—')}</td>
              <td>{String(decision['指标版本'] ?? '—')}</td>
              <td>{String(decision['复核结论'] ?? '—')}</td>
              <td>{String(decision['复核状态'] ?? '—')}</td>
              <td>{eventText(decision['通知事件'])}</td>
              <td>{previousVersion(decision['上一条有效结果'])}</td>
              <td className="row-actions">
                <button
                  className="link"
                  type="button"
                  onClick={() => void post(`${ENDPOINT}/decisions/${decision['判定编号']}/review`, {
                    复核结论: '确认告警',
                  })}
                >
                  确认复核
                </button>
                <button
                  className="link"
                  type="button"
                  onClick={() => void post(`${ENDPOINT}/decisions/${decision['判定编号']}/review`, {
                    复核结论: '复核失败',
                  })}
                >
                  复核失败
                </button>
                <button
                  className="link"
                  type="button"
                  onClick={() => void post(`${ENDPOINT}/decisions/${decision['判定编号']}/consume`, {})}
                >
                  消费
                </button>
              </td>
            </tr>
          ))}
          {decisions.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={decisionColumns.length + 1}>暂无判定结果，可对规则执行重算</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条流告警规则记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
