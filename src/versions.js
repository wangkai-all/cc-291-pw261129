/**
 * 冷版本租约：进行中的查询/取回任务固定读某个版本，空间回收必须绕开它们。
 * 进程重启后内存租约自然消失，但那时也没有进行中的查询了。
 */
export class VersionPins {
  #counts = new Map();

  static key(partitionId, versionId) {
    return `${partitionId}::${versionId}`;
  }

  pin(partitionId, versionId) {
    const key = VersionPins.key(partitionId, versionId);
    this.#counts.set(key, (this.#counts.get(key) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = (this.#counts.get(key) ?? 1) - 1;
      if (next <= 0) this.#counts.delete(key);
      else this.#counts.set(key, next);
    };
  }

  isPinned(partitionId, versionId) {
    return (this.#counts.get(VersionPins.key(partitionId, versionId)) ?? 0) > 0;
  }

  count(partitionId, versionId) {
    return this.#counts.get(VersionPins.key(partitionId, versionId)) ?? 0;
  }

  list() {
    return [...this.#counts.entries()].map(([key, count]) => {
      const [partitionId, versionId] = key.split('::');
      return { partitionId, versionId, count };
    });
  }
}
