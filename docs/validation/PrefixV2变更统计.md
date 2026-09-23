# Prefix V2 变更文件与 Git diff 统计

远端 HEAD：`8ee9878b45e83726c79c2f1bc572f7cd711af939`。工作空间根目录无 .git；在独立克隆中先将索引设为本轮开始前的本地快照，再覆盖本轮文件，运行 `git diff --stat`。未创建 commit 或推送。统计比较的是本轮本地基线，不混入此前规划文档与远端的差异；不含此生成的统计说明本身。

## 修改文件

- `src/inferscale/config.py`
- `src/inferscale/features.py`
- `src/inferscale/ledger.py`
- `src/inferscale/models.py`
- `src/inferscale/policies.py`
- `src/inferscale/proxy.py`
- `src/inferscale/samples.py`
- `src/inferscale/benchmark/compare.py`
- `src/inferscale/benchmark/report.py`
- `src/inferscale/benchmark/spec.py`
- `src/inferscale/benchmark/__main__.py`
- `tests/integration/test_benchmark_cli.py`
- `tests/integration/test_p3_gateway.py`
- `tests/integration/test_prefix_v2.py`
- `tests/integration/test_server_smoke.py`
- `tests/unit/test_comparison_spec.py`
- `tests/unit/test_prefix_v2.py`
- `tests/unit/test_v2_plan.py`
- `scripts/package_release.py`
- `scripts/server_smoke.py`
- `configs/prefix-v2-comparison.example.json`
- `docs/PrefixV2实现与验收.md`
- `docs/PrefixV2方案审核与实现规划.md`
- `docs/代码实现规划.md`
- `deploy/P4实验运行.md`
- `deploy/PrefixV2六轮实验.md`
- `README.md`

## git diff --stat

```text
 README.md                                   |   6 +-
 configs/prefix-v2-comparison.example.json   |  39 ++++++++
 deploy/P4实验运行.md                    |   4 +
 deploy/PrefixV2六轮实验.md              |  77 +++++++++++++++
 docs/PrefixV2实现与验收.md             |  52 ++++++++++
 docs/PrefixV2方案审核与实现规划.md |   6 +-
 docs/代码实现规划.md                  |   8 +-
 scripts/package_release.py                  |   2 +-
 scripts/server_smoke.py                     |   6 +-
 src/inferscale/benchmark/__main__.py        | 128 +++++++++++++++++++++----
 src/inferscale/benchmark/compare.py         |  35 ++++++-
 src/inferscale/benchmark/report.py          |  76 +++++++++++++++
 src/inferscale/benchmark/spec.py            |  67 +++++++++++++
 src/inferscale/config.py                    |   5 +-
 src/inferscale/features.py                  |  42 ++++++++
 src/inferscale/ledger.py                    |   9 +-
 src/inferscale/models.py                    |  11 +++
 src/inferscale/policies.py                  |  36 ++++++-
 src/inferscale/proxy.py                     |   3 +
 src/inferscale/samples.py                   |  11 +++
 tests/integration/test_benchmark_cli.py     |  35 ++++++-
 tests/integration/test_p3_gateway.py        |   7 +-
 tests/integration/test_prefix_v2.py         |  82 ++++++++++++++++
 tests/integration/test_server_smoke.py      |   9 +-
 tests/unit/test_comparison_spec.py          | 142 ++++++++++++++++++++++++++++
 tests/unit/test_prefix_v2.py                | 127 +++++++++++++++++++++++++
 tests/unit/test_v2_plan.py                  |  55 +++++++++++
 27 files changed, 1026 insertions(+), 54 deletions(-)
```

补丁：`dist/inferscale-prefix-v2.patch`。原有配置、workloads、runner、pyproject.toml 和 uv.lock 的 SHA-256 与改动前一致。
