# Аудит хардкода и производительности — 2026-11-24

Read-only аудит всего репозитория силами 4 параллельных ревьюеров + лид (корневые/infra файлы).
Файлы не изменялись. Все ссылки вида `file:line` проверены чтением кода.
«Подтверждено» = прочитаны обе стороны; «Спекулятивно» = влияние/намерение требует проверки.

Итог по количеству: **12 high · 26 med · ~55 low** (после слияния пересечений) + **15 перф-находок**.
Проект в целом дисциплинирован: секретов нет, SQL параметризован, N+1 нет, кэш-гигиена хорошая.
Главные классы проблем: (1) скоринговые константы вне `config.py` → не попадают в хэши инвалидации
→ тихий дрейф баллов/кэша; (2) Python↔Rust и Python↔lua↔SQL дубли без паритет-тестов.

---

## 1. HIGH — хардкод, ломающий инвалидацию кэша или паритет

### H1. Масштаб баллов `* 25.0` продублирован и не хэшируется
`precompute/grid.py:328` и `precompute/surface.py:356` — `min(effective_count / cap, 1.0) * 25.0`.
Константа 25.0 не в `config.py` и не входит в `score_hash` (там есть `CAPS`, но не масштаб).
Изменение меняет все баллы без инвалидации; грубая/точная сетки могут разъехаться.
**Фикс:** `CATEGORY_SCORE_MAX_POINTS = 25.0` в config, включить в `score_params`, импортировать в обоих.

### H2. `_MIN_DENSITY_AREA_RATIO = 0.25` и `_DENSITY_NORMALIZED_CATEGORIES` продублированы
`precompute/grid.py:287-288` (tuple) vs `precompute/surface.py:51-52` (set) — одинаковые значения
в двух скорерах, вне config и хэша. Влияют на нормализацию плотности.
**Фикс:** централизовать в config (`MIN_DENSITY_AREA_RATIO`, `DENSITY_NORMALIZED_CATEGORIES`) + хэш.

### H3. Политика классификации v4 вне classification-хэша
`precompute/daily_access_classify.py:468` `_FOOTPRINT_OVERLAP_SHARE = 0.5` (объединение дубликатов)
и `:447-467` `_EVIDENCE_RANK` (какое свидетельство побеждает в группе) — меняют входы v4 reach/score,
но не входят в `config.daily_access_classification_params()` → не попадают в `reach_hash`.
**Фикс:** перенести в config и добавить в classification params.

### H4. Транзитные тир-юниты вне config и продублированы в Rust без паритет-теста
`transit/service.py:39-45` `BUS_FREQUENCY_TIER_UNITS` (5/4/3/2/1), `:144-157` границы headway
15/30/60/120, `:160-179` mode-tier units, `:182-205` веса /16 /24 /32 ×0.6/0.4/0.2/0.1/0.1.
В `config.py` транспортных тир-констант **вообще нет** (только v4 `DAILY_ACCESS_TRANSPORT_TIER_QUALITY`),
и `transit_config_hash()` их не покрывает. Те же таблицы продублированы в Rust
`walkgraph/src/gtfs/mod.rs:944-955, 1801-1849` — это production-путь. Python↔Rust паритет-теста нет
(в отличие от v4 daily-access). Изменение на любой стороне проходит обе тестовые сюиты.
**Фикс:** единый источник (константы в config → конфиг-JSON для gtfs-refresh) + паритет-тест
в стиле `tests/test_daily_access_native.py`.

### H5. Список landuse-классов существует в 3 копиях, config-копия — мёртвый код
`config.py:804` `LANDUSE_CONTEXT_CLASSES` (никем не импортируется), `db_postgis/reads.py:65-67`
`_LANDUSE_CONTEXT_VALUES` (живая копия в SQL-фильтре :468), `osm2pgsql_livability.lua:38-45`
(третья копия в импорте). Правка config ничего не меняет; правка reads/lua не инвалидирует ничего.
**Фикс:** импортировать config-константу в reads.py + drift-проверка lua↔config.

### H6. Whitelist тегов в lua шире, чем `config.TAGS`
`osm2pgsql_livability.lua:59-61` добавляет ветку `tags.healthcare and tags.healthcare ~= 'no'`
(в config `TAGS["healthcare"]` её нет); `:27-31, 71-73` добавляет park-значения
`garden/common/village_green/recreation_ground` и `boundary == 'national_park'`
(config `TAGS["parks"]["leisure"]` = {park, playground, nature_reserve}).
Импортёр молча импортирует шире, чем задекларировано; смена TAGS не меняет импорт.
**Фикс:** перенести списки в config + drift-тест (или генерация lua-секции из config).

---

## 2. MED — хардкод

### Бэкенд/воркеры
- **M1** `pmtiles_bake_worker.py:333-338` `_resolution_for_zoom` (≥10→5000, ≥8→10000, else 20000) и
  `fine_vector_pmtiles_worker.py:31-36` `_FINE_GRID_RESOLUTIONS_BY_ZOOM` {12:2500, 13:1000, 14:500, 15:250/100/50}
  дублируют profile `surface_zoom_breaks` из config — при смене лестницы тайлы пекутся со старым маппингом.
- **M2** `fine_vector_pmtiles_worker.py:383` декод-константы `65535` и `/100.0` (дубли `DAILY_ACCESS_UINT16_MISSING`,
  `DAILY_ACCESS_SCORE_STEP`); `:291` литерал `"daily-access-v4"` (дубль `DAILY_ACCESS_SCORE_MODEL`).
- **M3** `precompute/daily_access_score.py:117` и `daily_access_runtime.py:66` — total clamp `100.0` в двух местах,
  не константа config и не в `daily_access_score_params()`.
- **M4** `precompute/bake_pmtiles.py:71-81` зум-константы (`DEFAULT_MIN_ZOOM=5`, `SOURCE_MAX_ZOOM=15`,
  `AMENITY/TRANSPORT_MIN_ZOOM=9`, `NOISE_MIN_ZOOM=8`, `NOISE_MAX_ZOOM_DEFAULT=13`) вне config и `render_hash`;
  `DEFAULT_BBOX=(-11.0, 51.3, -5.3, 55.5)` захардкожен в `bake_pmtiles.py:75` и `noise_artifacts/bake.py:20`
  вместо вывода из геометрии study-area.
- **M5** URL-путь `"/exports/transport-reality.zip"` в трёх местах: `display_layers.py:150`, `_rows.py:391`,
  `serve_routes.py:39` — дублирует `transit/export.py:25` `ZIP_FILENAME`; переименование архива даст тихий 404.
- **M6** `overture/merge.py:11-13` радиусы мерджа 35/75/10 вне config (инвалидация только ручным
  бампом `AMENITY_MERGE_ALGO_VERSION`).
- **M7** `basemap.py:26` имя области (дубль `OSM_EXTRACT_NAME`) и `:69` литерал `"v4"`
  (дубль `PROTOMAPS_TILE_SCHEMA_MAJOR`).
- **M8** Road-классы ×3: `db_postgis/reads.py:35`, `osm2pgsql_livability.lua:47-51`,
  ключи `config.ROAD_PROXIMITY_CLASS_SETTINGS` (motorway/trunk/primary).
- **M9** `scripts/fetch_datasets.py:64-135` повторяет все пути датасетов из config (9+ литералов);
  Geofabrik URL дублируется в `scripts/refresh_osm.py:19-21`. Скрипт намеренно stdlib-only, но дрейф не проверяется.
- **M10** `noise_artifacts/runner.py:194` fallback `0.1` противоречит дефолту config `1.0`
  (и параметр-дефолты `0.1` в builder/dissolve/resolve/geometry).

### Фронтенд (фикс потребует `npm run build --prefix frontend`)
- **M11** `runtime_contract.js:69` `"daily-access-v4"` + `popups/grid_popup.js:132` (дубль `DAILY_ACCESS_SCORE_MODEL`).
- **M12** Zoom-фоллбэки: `runtime_contract.js:250,378,386,393` и `main.js:82,1662,1664` — 19/6/20000 и
  `MIN_ZOOM=5` дублируют `config.py:410-412,335`; min-zoom вообще не приходит из runtime-JSON.
- **M13** `runtime_contract.js:532` — фоллбэк `"http://127.0.0.1:8000"` (захардкожен protocol+host+port;
  в проде передаётся `window.location.origin`, но тихий фоллбэк — ловушка).
- **M14** `transport_filters.js:48-53` — метки частот «≤15 / 16-30 / 31-60 / 61-120 / >120» дублируют
  `transit/service.py:147-156` и Rust `mod.rs:1808-1817`; смена порогов тихо разъедет подписи оверлея.
- **M15** `popups/grid_popup.js:61,122,139-170` — тексты/фоллбэки «50 м», «250 м» дублируют
  `CANONICAL_BASE_RESOLUTION_M=50`, `DAILY_ACCESS_SNAP_RADIUS_M/LAND_BAND_M=250`.

### Rust (паритет)
- **M16** `walkgraph/src/gtfs/mod.rs:944-955` `bus_subtier_for_mask` дублирует `transit/service.py:20-26,85-93`
  (словарь строк отражён и во фронте) — без паритет-теста.
- **M17** `walkgraph/src/pbf.rs:33,37` `RECREATION_HIGHWAYS`/`EDGE_FLAG_RECREATION` дублируют
  `config.py:686-687` (частично защищено v4 parity-тестом).
- **M18** `walkgraph/src/main.rs:21` `FORMAT_VERSION=4` дублирует `config.py:188`
  (защищено громким фейлом в `network/loader.py:340,360-363` — поздно, но не тихо).

---

## 3. LOW — кратко (полный список в отчётах тиммейтов)

- `main.py:6-7` — host/port дублируют config (`DEFAULT_SERVER_HOST/PORT`).
- `display_layers.py:31` `DISPLAY_LAYERS_VERSION=2` — версия вне config (нарушение конвенции);
  `:165` `"mode_aware_scoring_enabled": False` хардкодом (возможно намеренно — задокументировать).
- Категорийные кортежи ×4: `daily_access_score.py:33`, `bake_pmtiles.py:82`,
  `fine_vector_pmtiles_worker.py:37,386` — зеркалят порядок `config.TAGS/CAPS` (пересортировка config тихо сдвинет каналы).
- `precompute/__init__.py:223` и `service_deserts.py:27` — литерал `"gtfs_direct"` (дубль `TAGS["transport"]["source"]`).
- `precompute/valhalla_reachability.py:88` — `"EPSG:4326"/"EPSG:2157"` строками вместо `DISPLAY_CRS/TARGET_CRS`;
  `_rows.py:351-352` — `"EPSG:4326"` в legacy noise-хэше (хэш-безопасно).
- `grid.py:289` — мёртвый `_WEIGHTED_UNIT_CATEGORIES` (дубль `amenity_tiers.py:34`); дубль regex
  `_NON_ALNUM_RE` (`amenity_tiers.py:33` / `daily_access_classify.py:52`).
- `bake_pmtiles.py:727` — `zoom_bounds_for_resolution(5000)` без `profile` (всегда full-лестница).
- Локальные версии форматов: `surface.py:47`, `reachability_arrays.py:13`, `noise/*` legacy
  (безопасно — внутри хэш-неймспейсов, но вне конвенции).
- Фронтенд: кэш-баст `?v=2026-07-06-2` ×3 в `static/index.html:8,9,176` (ручной бамп; лучше build_key);
  `transport_reality_popup.js:101,109` (« / 5», «Friday 16:00…»); `grid_debug.js:29` `http://localhost`;
  таймауты fetch 45000/1200/15000 в `main.js:85-88`; `maxWidth: 320px` и т.п. — display-only.
- Rust: CLI-дефолты `main.rs:124-128,152-155` (Python всегда передаёт явно); тестовые `default_settings()`
  `mod.rs:2281-2296`; `GRID_CELL_M=250` (`daily_access/mod.rs:51`, Speculative); REASON-коды
  (`snap.rs:20-23`) — покрыты паритет-путём.
- `scripts/bench_valhalla.py:194-205` — URL :8002, workers 8, 33 768 400 ячеек.
- `daily_access_reference_run.py:51` — bbox отличается от test-profile bbox в config.
- `db_postgis/engine.py:17-19` — pool tuning; `gtfs_download.py:23-27` — таймауты;
  `classification.py:36,42` — `school_bucket_share >= 0.9`; `network/loader.py` и
  `local_osm_import/osm2pgsql.py` — дефолты-дубликаты (fallback-пути).
- `scripts/win/*.cmd` — пути `%USERPROFILE%\miniforge3`, `.\.venv\Scripts\python.exe` (все с документированными overrides).
- `noise` metric whitelist `{Lden, Lnight}` ×3: `noise/normalize.py:8`, `noise_artifacts/dev_fast.py:50`,
  `ogr_ingest.py:1501-1502,2573-2574`.
- Док-дрейф: `repo_map.md` §8 пишет `NOISE_OGR2OGR_TIMEOUT_SECONDS=300`, в коде `900`.
- `requirements.txt` — только нижние границы (`>=`), верхних пинов нет (воспроизводимость, не хардкод).

---

## 4. Оптимизации (ранжированы по impact × вероятность)

| # | Impact | Где | Что | Усилие |
|---|--------|-----|-----|--------|
| P1 | high | `walkgraph/src/daily_access/mod.rs:548` | `origins.binary_search` в самом внутреннем цикле скоринга v4 (на ячейку×запись×kind, origins до 1M+) → заранее построить `Vec<u32>` node→row (в `reach.rs:79` это уже сделано) | малое |
| P2 | high | `walkgraph/src/reachability.rs:194,266` | Новый `HashMap<u32,f32>` (SipHash) на каждую origin-Dijkstra (сотни тысяч origins) → thread-local `Vec<f32>` + epoch-массив | среднее |
| P3 | med-high | `walkgraph/src/reachability.rs:288-301` | `0.5_f32.powf(...)` + re-проверка валидности на каждую (node,category) → предвычислить `k=ln(0.5)/half`, `exp2f`, вынести валидацию | малое |
| P4 | med | `db_postgis/daily_access_green.py:595-612` | Per-row `copy.write_row` в Python-цикле по миллионам рёбер → векторный буфер одним `copy.write` (10–50×) | малое-среднее |
| P5 | med | `serve_from_db.py:859-860` | Полный `_load_state()` (2+ DB-запроса) на **каждый** `/api/runtime` запрос → TTL 10–30 с / дешёвая проверка build_key | малое |
| P6 | med | `pmtiles_bake_worker.py:355-423,462,503` | До 6 отдельных `ST_AsMVT`-запросов на тайл + `gzip` level 9 → один multi-CTE запрос; `compresslevel=6` (то же в `bake_pmtiles.py:587`) | среднее / тривиально |
| P7 | med-low | `grid.py:407-452` + `reachability_arrays.py:228-256` | Per-cell `np.searchsorted` + пересборка dict на каждую ячейку → однократный node→row индекс | среднее |
| P8 | med-low | `fine_vector_pmtiles_worker.py:549-590` | Per-cell Python-цикл с полигональным клипом и `tolist()` на каждый fine-тайл → не клипить внутренние ячейки, батчить фичи | среднее |
| P9 | low-med | `surface.py:1084-1087` | Неограниченные per-shard dict-кэши `FineSurfaceRuntime` → `ByteLRU` как в v4 (Speculative: зависит от трафика) | малое-среднее |
| P10 | low | `amenity_clusters.py:49-86` | Python union-find по `query_radius` → `scipy.sparse.csgraph.connected_components`; + `:133-137` повторная фильтрация на категорию | малое-среднее |
| P11 | low | `daily_access/mod.rs:562` | Per-cell-per-kind аллокация `Vec<Vec<...>>` + sort → scratch-буферы на rayon-поток | малое-среднее |
| P12 | low | `pmtiles_bake_worker.py:296-304` | `ST_Area(ST_Transform(...))` пересчитывается per tile per row → пре-вычислить колонкой | малое |
| P13 | low | `walkgraph/src/graph.rs:130-155`, `reachability.rs:350-392` | Миллионы мелких `write_all` → батч-буферы (build-time) | малое |
| P14 | low | `network.py:701-722` | Три матрицы считаются отдельными subprocess-прогонами, каждый перечитывает граф → один прогон на чанк с несколькими weight-файлами (Speculative) | среднее |

**Спекулятивное** (нужен профиль): P2/P9/P14 по абсолютному выигрышу; `daily_access_reach.py`
(reference-путь, в проде Rust) и decode-циклы — не трогать.

---

## 5. Что чисто (проверено, находок нет)

- **Секреты**: ни одного пароля/токена/ключа в коде, фронте и Rust. `.env` в `.gitignore`;
  CI использует `secrets.DATABASE_URL`; пины версий в CI тянутся из `config.py`.
- **SQL**: все запросы параметризованы (`:param`), f-string SQL — только quoted identifiers/внутренние
  имена таблиц. Инъекций нет. **N+1 в `db_postgis/reads.py` нет** — все загрузки bulk/стриминг.
- **Пути**: в Rust и фронте нет абсолютных путей; setup-скрипты всё берут из `.env`;
  `study_area.py` использует `TARGET_CRS` из config.
- **Кэш-гигиена**: tiers/cache корректны (schema version + hash + package majors), surface manifest
  ключуется `score_hash`+`render_hash`, decay-параметры передаются из config.
- **Паритет v4 daily-access**: покрыт настоящим Python↔Rust тестом (`test_daily_access_native.py:87`) —
  константы score.rs/reach.rs/snap.rs там LOW-риска.
- CRS-определение `walkgraph/src/surface/itm.rs` (EPSG:2157) — легитимная константа, не дубль.
- `WALK_EXCLUDED/BIKE_EXCLUDED` в `pbf.rs` — у них нет Python-близнеца, не паритет-проблема.

---

## 6. Рекомендуемый план фиксов

**Фаза 1 — безопасно, без изменения баллов и пересборки Rust (рекомендуется сразу):**
H1–H3 (константы в config + хэши), M2/M3/M5/M7/M10/M11–M15 (импорт/дедуп констант),
P5 (TTL на /api/runtime), P6-gzip (level 6), LOW-дедупы (host/port, regex, мёртвый код).
*Внимание:* правки фронта требуют `npm run build --prefix frontend` + коммит `static/dist/`.

**Фаза 2 — паритет и единый источник (средний риск, нужны тесты):**
H4 (transit tiers: config + config-JSON для Rust + паритет-тест), H5/H6/M8 (landuse/road/tags:
единый источник + drift-тест lua↔TAGS), M1/M4 (zoom-лестницы из profile), M6/M9 (радиусы/пути в config).

**Фаза 3 — Rust-оптимизации (после профилирования; любое изменение скоринга требует
полного перепрекомпьюта и бампа `SCORING_MODEL_VERSION`):**
P1 → P3 → P2, затем P4/P6-CTE/P7/P8 по итогам профиля.

**Важно:** любое изменение скоринговых констант меняет баллы → нужен новый прекомпьют;
изменения Rust-бинаря → `cargo build --release` + повторная сборка walkgraph-кэшей.

---

## 7. Перепроверка и фиксы — 2026-11-24

### Сделано
- **Rail/tram-тиры в Rust** (аудит пропустил): `walkgraph/src/gtfs/mod.rs` считает `transport_mode_tier`
  (rail = 4, tram = 5) и пишет его в CSV; в Rust и в `transit/service.py` тир действует только при
  `public_departures_30d > 0`. Раньше тиры были лишь в Python-пути, который вызывают только тесты.
  Локальные фиды на 2026-10-27: изменилась 371 остановка (243 rail → 4, 128 tram → 5), остальные поля без отличий.
- **H4, M16**: `tests/test_transit_native_parity.py` сравнивает Python и Rust по всем полям (на старом бинаре падает).
- **CI**: walkgraph собирается до Python-тестов; раньше паритет-тесты там всегда пропускались.
- **H1, H2**: константы живут в `precompute/grid.py`, `surface.py` их импортирует. В хэши не включены (см. handoff).
- **H5, H6, M1, M8, M9**: `tests/test_config_drift.py` ловит расхождение lua, воркеров и путей датасетов с config.
- **P1**: строка узла ищется один раз на ячейку. Блок 160 000 ячеек: выход побайтно тот же,
  второй прогон 16.8 → 15.9 с (~5 %), то есть не «high impact».

### Поправки к аудиту
- Счётчики: 6 high, а не 12; 14 перф-находок, а не 15.
- H5: правка lua инвалидирует импорт — содержимое файла входит в `importer_config_hash`.
- H6: не баг. Шире `TAGS` намеренно: v4 нужны эти типы, v3 фильтрует через `legacy_v3_amenity_category`.
- M5: тихого 404 нет, сервер берёт файл через `ZIP_FILENAME`. M10: fallback `0.1` недостижим.
- P3: для `exp2` нужен `k = -1/half`; `ln(0.5)/half` — это для `exp`. P2/P3 ускоряют путь v3.
- Фаза 1 меняет `score_hash`/`reach_hash`, поэтому требует прекомпьюта.

### Найдено, не исправлено (нужно решение: меняет баллы)
1. **Железная дорога выпала из railway proximity.** `transit/gtfs_zip.py` `route_mode`: GTFS-тип 2 (rail) считается
   `bus`, а `transit/railway_corridors.py` использует это в проде. Во всех сборках в БД `route_modes_json = ["tram"]`,
   85.7 км Luas; heavy rail штрафа не даёт.
2. **`friday_evening_deps`** в Rust заполняется только для автобусов. На units rail/tram уже не влияет, но попап показывает 0.

Оба закреплены как `expectedFailure` в `tests/test_transit_native_parity.py`.

### Handoff агенту route network (его файлы)
1. **Обязательно:** `config.TRANSIT_REALITY_ALGO_VERSION` 10 → 11. Без бампа новые тиры не попадут в сборку.
2. `build_hashes_for_import` теряет `railway_proximity_{active_modes,full_penalty_distance_m,zero_penalty_distance_m,max_penalty}`
   в `score_hash`; затронут только v3. Нужны общие функции параметров для обеих хэш-функций и тест.
3. В хэши: `CATEGORY_SCORE_MAX_POINTS`, `MIN_DENSITY_AREA_RATIO`, `DENSITY_NORMALIZED_CATEGORIES` (score),
   `_FOOTPRINT_OVERLAP_SHARE`, `_EVIDENCE_RANK` (`daily_access_classification_params`).
4. `LANDUSE_CONTEXT_ALGO_VERSION` нигде не используется → в `render_params`; `reads.py` должен импортировать
   `LANDUSE_CONTEXT_CLASSES`.
5. `compresslevel=6` для `gzip.compress` в bake-файлах; M2 в `fine_vector_pmtiles_worker.py`; M11 во фронте.
6. `repo_map.md`: версия 11, rail/tram-тиры в Rust, два новых теста, `NOISE_OGR2OGR_TIMEOUT_SECONDS` 300 → 900.

Пункты 1–4 меняют хэши: один общий прекомпьют после всех правок.
