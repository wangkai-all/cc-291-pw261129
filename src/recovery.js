/**
 * 进程恢复：把“磁盘实况”与“目录记录”对齐。
 *
 * 恢复原则：只做保守修正——能够确认副本存在且摘要正确的，继续使用；
 * 不能确认的，退回仍有校验依据的另一层；两层都不可用时报告 NO_REPLICA，
 * 绝不擅自删掉最后一个副本的目录记录。
 */
import { nowIso, sha256File } from './util.js';

export class Recovery {
  constructor({
    catalog, cold, hot, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.log = log;
  }

  async run({ verifyCold = false, actor = 'system' } = {}) {
    const startedAt = nowIso();
    const report = {
      startedAt,
      finishedAt: null,
      interrupted: await this.catalog.recoverInterrupted(),
      actions: [],
      problems: [],
    };

    for (const dataset of this.catalog.listDatasets()) {
      for (const file of dataset.files) {
        const hotPath = file.hot?.path ?? null;
        const hotSha = file.hot?.sha256 ?? null;
        if (hotPath) {
          const actual = await sha256File(hotPath).catch(() => null);
          if (actual !== hotSha) {
            const hasCold = Boolean(file.cold);
            this.catalog.markHotMissing(dataset.id, file.id, {
              coldExists: hasCold,
              detail: actual === null ? '（热文件不存在）' : '（热文件摘要不一致）',
            });
            report.actions.push({
              datasetId: dataset.id, fileId: file.id,
              action: hasCold ? 'hot-invalidated' : 'no-replica',
              detail: hasCold
                ? '热副本缺失或损坏，已回退到冷副本'
                : '热副本缺失且无冷副本，需重新迁移或重建',
            });
          }
        }
        if (verifyCold && file.cold) {
          const result = await this.cold.verifyObject(file.cold.partitionId, file.cold.versionId, file.cold.objectId);
          if (!result.ok) {
            if (file.hot) {
              this.catalog.markFileError(dataset.id, file.id, '冷副本校验失败，继续使用热副本', {
                evidence: { ...result },
                kind: 'recovery.cold-invalid',
              });
              report.actions.push({
                datasetId: dataset.id, fileId: file.id, action: 'cold-invalid-hot-kept',
                detail: '冷副本校验失败，热副本仍可用',
              });
            } else {
              this.catalog.markFileError(dataset.id, file.id, '冷副本校验失败且没有热副本', {
                evidence: { ...result },
                kind: 'recovery.no-replica',
              });
              report.actions.push({
                datasetId: dataset.id, fileId: file.id, action: 'no-replica',
                detail: '冷副本校验失败，需要重新迁移',
              });
            }
          }
        }
        const current = this.catalog.findFile(dataset.id, file.id).file;
        if (!current.hot && !current.cold) {
          report.problems.push({
            datasetId: dataset.id, datasetName: dataset.name, fileId: file.id, fileName: file.name,
            code: 'NO_REPLICA', message: '没有任何可读副本',
          });
        }
      }
    }
    report.finishedAt = nowIso();
    if (report.actions.length) {
      this.catalog.addEvent({
        kind: 'recovery.completed',
        datasetId: null,
        reason: `恢复扫描：${report.actions.length} 项修正，${report.problems.length} 项问题`,
        evidence: { actions: report.actions, verifyCold },
        actor,
      });
      await this.catalog.flush();
    }
    return report;
  }
}
