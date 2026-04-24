use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime};
use zip::ZipArchive;

const REQUIRED_GTFS_FILENAMES: [&str; 4] =
    ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"];
const GTFS_DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    format_description!("[year][month][day]");
const ISO_DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day]");

#[derive(Debug, Deserialize)]
struct GtfsRefreshConfig {
    analysis_date: String,
    analysis_window_days: usize,
    service_desert_window_days: usize,
    lookahead_days: usize,
    import_fingerprint: String,
    reality_fingerprint: String,
    school_keywords: Vec<String>,
    school_am_start_hour: u8,
    school_am_end_hour: u8,
    school_pm_start_hour: u8,
    school_pm_end_hour: u8,
    commute_am_start_hour: u8,
    commute_am_end_hour: u8,
    commute_pm_start_hour: u8,
    commute_pm_end_hour: u8,
    bus_daytime_start_hour: u8,
    bus_daytime_end_hour: u8,
    friday_evening_start_hour: u8,
    friday_evening_end_hour: u8,
    feeds: Vec<FeedInput>,
}

#[derive(Debug, Deserialize, Serialize)]
struct FeedInput {
    feed_id: String,
    label: String,
    zip_path: PathBuf,
    feed_fingerprint: String,
    source_url: Option<String>,
}

#[derive(Debug, Clone)]
struct RunSettings {
    analysis_date: Date,
    analysis_window_days: usize,
    service_desert_window_days: usize,
    lookahead_days: usize,
    school_keywords: BTreeSet<String>,
    school_am_start_hour: u8,
    school_am_end_hour: u8,
    school_pm_start_hour: u8,
    school_pm_end_hour: u8,
    commute_am_start_hour: u8,
    commute_am_end_hour: u8,
    commute_pm_start_hour: u8,
    commute_pm_end_hour: u8,
    bus_daytime_start_hour: u8,
    bus_daytime_end_hour: u8,
    friday_evening_start_hour: u8,
    friday_evening_end_hour: u8,
    import_fingerprint: String,
    reality_fingerprint: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct StopInfo {
    stop_code: Option<String>,
    stop_name: String,
    stop_desc: Option<String>,
    stop_lat: f64,
    stop_lon: f64,
    parent_station: Option<String>,
    zone_id: Option<String>,
    location_type: Option<i32>,
    wheelchair_boarding: Option<i32>,
    platform_code: Option<String>,
}

#[derive(Debug, Clone)]
struct RouteInfo {
    agency_id: Option<String>,
    route_short_name: Option<String>,
    route_long_name: Option<String>,
    route_desc: Option<String>,
    route_type: Option<i32>,
    route_url: Option<String>,
    route_color: Option<String>,
    route_text_color: Option<String>,
}

#[derive(Debug, Clone)]
struct TripInfo {
    route_id: String,
    service_id: String,
    trip_headsign: Option<String>,
    trip_short_name: Option<String>,
    direction_id: Option<i32>,
    block_id: Option<String>,
    shape_id: Option<String>,
    mode: String,
}

#[derive(Debug, Clone)]
struct CalendarService {
    monday: i32,
    tuesday: i32,
    wednesday: i32,
    thursday: i32,
    friday: i32,
    saturday: i32,
    sunday: i32,
    start_date: Date,
    end_date: Date,
}

#[derive(Debug, Clone)]
struct CalendarDateException {
    service_id: String,
    service_date: Date,
    exception_type: i32,
}

#[derive(Debug, Default, Clone, Serialize)]
struct TimeBucketCounts {
    morning: u32,
    afternoon: u32,
    offpeak: u32,
}

#[derive(Debug)]
struct FeedDataset {
    feed_id: String,
    stops: BTreeMap<String, StopInfo>,
    routes: BTreeMap<String, RouteInfo>,
    trips: BTreeMap<String, TripInfo>,
    calendar_services: BTreeMap<String, CalendarService>,
    calendar_dates: Vec<CalendarDateException>,
    stop_service_occurrences: HashMap<(String, String, String, String), u32>,
    stop_service_time_occurrences: HashMap<(String, String, String, String, Option<i32>), u32>,
    service_time_buckets: HashMap<String, TimeBucketCounts>,
    service_route_ids: HashMap<String, BTreeSet<String>>,
    service_route_modes: HashMap<String, BTreeSet<String>>,
    service_keywords: HashMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone)]
struct ServiceWindow {
    dates_30d: Vec<Date>,
    dates_7d: Vec<Date>,
    weekday_dates: u32,
    weekend_dates: u32,
}

#[derive(Debug, Clone)]
struct ServiceClassification {
    feed_id: String,
    service_id: String,
    school_only_state: String,
    route_ids: Vec<String>,
    route_modes: Vec<String>,
    reason_codes: Vec<String>,
    time_bucket_counts: TimeBucketCounts,
}

#[derive(Debug, Clone)]
struct StopServiceSummary {
    feed_id: String,
    stop_id: String,
    public_departures_7d: u32,
    public_departures_30d: u32,
    school_only_departures_30d: u32,
    weekday_morning_peak_deps: f64,
    weekday_evening_peak_deps: f64,
    weekday_offpeak_deps: f64,
    saturday_deps: f64,
    sunday_deps: f64,
    friday_evening_deps: f64,
    transport_score_units: u32,
    bus_daytime_deps: f64,
    bus_daytime_headway_min: Option<f64>,
    bus_frequency_tier: Option<String>,
    bus_frequency_score_units: u32,
    last_public_service_date: Option<Date>,
    last_any_service_date: Option<Date>,
    route_modes: Vec<String>,
    route_ids: Vec<String>,
    reason_codes: Vec<String>,
    bus_active_days_mask_7d: Option<String>,
    bus_service_subtier: Option<String>,
    is_unscheduled_stop: bool,
    has_exception_only_service: bool,
    has_any_bus_service: bool,
    has_daily_bus_service: bool,
}

#[derive(Debug, Clone)]
struct GtfsStopReality {
    source_ref: String,
    stop_name: Option<String>,
    feed_id: String,
    stop_id: String,
    source_status: String,
    reality_status: String,
    school_only_state: String,
    public_departures_7d: u32,
    public_departures_30d: u32,
    school_only_departures_30d: u32,
    weekday_morning_peak_deps: f64,
    weekday_evening_peak_deps: f64,
    weekday_offpeak_deps: f64,
    saturday_deps: f64,
    sunday_deps: f64,
    friday_evening_deps: f64,
    transport_score_units: u32,
    bus_daytime_deps: f64,
    bus_daytime_headway_min: Option<f64>,
    bus_frequency_tier: Option<String>,
    bus_frequency_score_units: u32,
    last_public_service_date: Option<Date>,
    last_any_service_date: Option<Date>,
    route_modes: Vec<String>,
    source_reason_codes: Vec<String>,
    reality_reason_codes: Vec<String>,
    lat: f64,
    lon: f64,
    bus_active_days_mask_7d: Option<String>,
    bus_service_subtier: Option<String>,
    is_unscheduled_stop: bool,
    has_exception_only_service: bool,
    has_any_bus_service: bool,
    has_daily_bus_service: bool,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    analysis_date: String,
    created_at: String,
    import_fingerprint: String,
    reality_fingerprint: String,
    feeds: Vec<FeedRunSummary>,
    derived_counts: BTreeMap<String, u64>,
    timings_ms: BTreeMap<String, u128>,
}

#[derive(Debug, Serialize)]
struct FeedRunSummary {
    feed_id: String,
    label: String,
    feed_fingerprint: String,
    source_path: String,
    raw_counts: BTreeMap<String, u64>,
}

struct FeedRawWriters {
    stops: CsvWriter,
    routes: CsvWriter,
    trips: CsvWriter,
    stop_times: CsvWriter,
    calendar_services: CsvWriter,
    calendar_dates: CsvWriter,
    counts: BTreeMap<String, u64>,
}

struct DerivedWriters {
    service_classification: CsvWriter,
    stop_summary: CsvWriter,
    gtfs_stop_reality: CsvWriter,
    counts: BTreeMap<String, u64>,
}

struct CsvWriter {
    writer: BufWriter<File>,
}

impl CsvWriter {
    fn new(path: &Path, headers: &[&str]) -> Result<Self, Box<dyn Error>> {
        let file = File::create(path)?;
        let mut writer = Self {
            writer: BufWriter::new(file),
        };
        writer.write_record(headers)?;
        Ok(writer)
    }

    fn write_record<I, S>(&mut self, fields: I) -> Result<(), Box<dyn Error>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut first = true;
        for field in fields {
            if !first {
                self.writer.write_all(b",")?;
            }
            first = false;
            write_csv_field(&mut self.writer, field.as_ref())?;
        }
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.writer.flush()?;
        Ok(())
    }
}

pub fn run_gtfs_refresh(config_json: &Path, out_dir: &Path) -> Result<(), Box<dyn Error>> {
    let config_started_at = Instant::now();
    let config_text = fs::read_to_string(config_json)?;
    let config: GtfsRefreshConfig = serde_json::from_str(&config_text)?;

    if out_dir.exists() {
        fs::remove_dir_all(out_dir)?;
    }
    fs::create_dir_all(out_dir)?;

    let settings = RunSettings {
        analysis_date: parse_iso_date(&config.analysis_date, "analysis_date")?,
        analysis_window_days: config.analysis_window_days,
        service_desert_window_days: config.service_desert_window_days,
        lookahead_days: config.lookahead_days,
        school_keywords: config
            .school_keywords
            .iter()
            .map(|keyword| normalize_name(Some(keyword.as_str())))
            .filter(|keyword| !keyword.is_empty())
            .flat_map(|keyword| {
                keyword
                    .split_whitespace()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect(),
        school_am_start_hour: config.school_am_start_hour,
        school_am_end_hour: config.school_am_end_hour,
        school_pm_start_hour: config.school_pm_start_hour,
        school_pm_end_hour: config.school_pm_end_hour,
        commute_am_start_hour: config.commute_am_start_hour,
        commute_am_end_hour: config.commute_am_end_hour,
        commute_pm_start_hour: config.commute_pm_start_hour,
        commute_pm_end_hour: config.commute_pm_end_hour,
        bus_daytime_start_hour: config.bus_daytime_start_hour,
        bus_daytime_end_hour: config.bus_daytime_end_hour,
        friday_evening_start_hour: config.friday_evening_start_hour,
        friday_evening_end_hour: config.friday_evening_end_hour,
        import_fingerprint: config.import_fingerprint.clone(),
        reality_fingerprint: config.reality_fingerprint.clone(),
        created_at: OffsetDateTime::now_utc().format(&Rfc3339)?,
    };

    let mut feed_datasets = Vec::new();
    let mut feed_summaries = Vec::new();
    let mut timings_ms = BTreeMap::new();
    timings_ms.insert(
        "config_prep".to_string(),
        config_started_at.elapsed().as_millis(),
    );

    for feed in &config.feeds {
        eprintln!("gtfs-refresh: parsing GTFS feed {}", feed.feed_id);
        let started_at = Instant::now();
        let raw_dir = out_dir.join("raw").join(&feed.feed_id);
        let (dataset, feed_summary) = parse_gtfs_feed(feed, &settings, &raw_dir)?;
        timings_ms.insert(
            format!("feed_parse_{}", feed.feed_id),
            started_at.elapsed().as_millis(),
        );
        feed_datasets.push(dataset);
        feed_summaries.push(feed_summary);
    }

    let derived_started_at = Instant::now();
    let mut all_service_classifications = Vec::new();
    let mut all_stop_summaries = Vec::new();
    for dataset in &feed_datasets {
        let service_windows = expand_service_windows(dataset, &settings);
        let dataset_classifications =
            classify_services(dataset, &settings.reality_fingerprint, &service_windows);
        let stop_summaries = summarize_gtfs_stops(
            dataset,
            &settings,
            &settings.reality_fingerprint,
            &service_windows,
            &dataset_classifications,
        );
        all_service_classifications.extend(dataset_classifications.into_values());
        all_stop_summaries.extend(stop_summaries);
    }
    timings_ms.insert(
        "service_derive".to_string(),
        derived_started_at.elapsed().as_millis(),
    );

    let reality_started_at = Instant::now();
    let reality_rows = derive_gtfs_stop_reality(
        &feed_datasets,
        &all_stop_summaries,
        &settings.reality_fingerprint,
        &settings.import_fingerprint,
    );
    timings_ms.insert(
        "stop_reality".to_string(),
        reality_started_at.elapsed().as_millis(),
    );

    eprintln!("gtfs-refresh: writing CSV artifacts");
    let derived_dir = out_dir.join("derived");
    let mut derived_writers = DerivedWriters::new(&derived_dir)?;
    for row in &all_service_classifications {
        derived_writers.write_service_classification(row, &settings)?;
    }
    for row in &all_stop_summaries {
        derived_writers.write_stop_summary(row, &settings)?;
    }
    for row in &reality_rows {
        derived_writers.write_gtfs_stop_reality(row, &settings)?;
    }
    derived_writers.flush()?;

    let summary = RunSummary {
        analysis_date: settings.analysis_date.to_string(),
        created_at: settings.created_at.clone(),
        import_fingerprint: settings.import_fingerprint.clone(),
        reality_fingerprint: settings.reality_fingerprint.clone(),
        feeds: feed_summaries,
        derived_counts: derived_writers.counts,
        timings_ms,
    };
    fs::write(
        out_dir.join("run_summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    eprintln!("gtfs-refresh: complete");
    Ok(())
}

impl FeedRawWriters {
    fn new(base_dir: &Path) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(base_dir)?;
        Ok(Self {
            stops: new_csv_writer(
                &base_dir.join("stops.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "stop_id",
                    "stop_code",
                    "stop_name",
                    "stop_desc",
                    "stop_lat",
                    "stop_lon",
                    "parent_station",
                    "zone_id",
                    "location_type",
                    "wheelchair_boarding",
                    "platform_code",
                    "created_at",
                ],
            )?,
            routes: new_csv_writer(
                &base_dir.join("routes.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "route_id",
                    "agency_id",
                    "route_short_name",
                    "route_long_name",
                    "route_desc",
                    "route_type",
                    "route_url",
                    "route_color",
                    "route_text_color",
                    "created_at",
                ],
            )?,
            trips: new_csv_writer(
                &base_dir.join("trips.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "route_id",
                    "service_id",
                    "trip_id",
                    "trip_headsign",
                    "trip_short_name",
                    "direction_id",
                    "block_id",
                    "shape_id",
                    "created_at",
                ],
            )?,
            stop_times: new_csv_writer(
                &base_dir.join("stop_times.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "trip_id",
                    "arrival_seconds",
                    "departure_seconds",
                    "stop_id",
                    "stop_sequence",
                    "pickup_type",
                    "drop_off_type",
                    "created_at",
                ],
            )?,
            calendar_services: new_csv_writer(
                &base_dir.join("calendar_services.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                    "start_date",
                    "end_date",
                    "created_at",
                ],
            )?,
            calendar_dates: new_csv_writer(
                &base_dir.join("calendar_dates.csv"),
                &[
                    "feed_fingerprint",
                    "feed_id",
                    "service_id",
                    "service_date",
                    "exception_type",
                    "created_at",
                ],
            )?,
            counts: BTreeMap::new(),
        })
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.stops.flush()?;
        self.routes.flush()?;
        self.trips.flush()?;
        self.stop_times.flush()?;
        self.calendar_services.flush()?;
        self.calendar_dates.flush()?;
        Ok(())
    }

    fn bump(&mut self, key: &str) {
        *self.counts.entry(key.to_string()).or_insert(0) += 1;
    }
}

impl DerivedWriters {
    fn new(base_dir: &Path) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(base_dir)?;
        Ok(Self {
            service_classification: new_csv_writer(
                &base_dir.join("service_classification.csv"),
                &[
                    "reality_fingerprint",
                    "feed_id",
                    "service_id",
                    "school_only_state",
                    "route_ids_json",
                    "route_modes_json",
                    "reason_codes_json",
                    "time_bucket_counts_json",
                    "created_at",
                ],
            )?,
            stop_summary: new_csv_writer(
                &base_dir.join("gtfs_stop_service_summary.csv"),
                &[
                    "reality_fingerprint",
                    "feed_id",
                    "stop_id",
                    "public_departures_7d",
                    "public_departures_30d",
                    "school_only_departures_30d",
                    "weekday_morning_peak_deps",
                    "weekday_evening_peak_deps",
                    "weekday_offpeak_deps",
                    "saturday_deps",
                    "sunday_deps",
                    "friday_evening_deps",
                    "transport_score_units",
                    "bus_daytime_deps",
                    "bus_daytime_headway_min",
                    "bus_frequency_tier",
                    "bus_frequency_score_units",
                    "last_public_service_date",
                    "last_any_service_date",
                    "bus_active_days_mask_7d",
                    "bus_service_subtier",
                    "is_unscheduled_stop",
                    "has_exception_only_service",
                    "has_any_bus_service",
                    "has_daily_bus_service",
                    "route_modes_json",
                    "route_ids_json",
                    "reason_codes_json",
                    "created_at",
                ],
            )?,
            gtfs_stop_reality: new_csv_writer(
                &base_dir.join("gtfs_stop_reality.csv"),
                &[
                    "reality_fingerprint",
                    "import_fingerprint",
                    "source_ref",
                    "stop_name",
                    "feed_id",
                    "stop_id",
                    "source_status",
                    "reality_status",
                    "school_only_state",
                    "public_departures_7d",
                    "public_departures_30d",
                    "school_only_departures_30d",
                    "weekday_morning_peak_deps",
                    "weekday_evening_peak_deps",
                    "weekday_offpeak_deps",
                    "saturday_deps",
                    "sunday_deps",
                    "friday_evening_deps",
                    "transport_score_units",
                    "bus_daytime_deps",
                    "bus_daytime_headway_min",
                    "bus_frequency_tier",
                    "bus_frequency_score_units",
                    "last_public_service_date",
                    "last_any_service_date",
                    "bus_active_days_mask_7d",
                    "bus_service_subtier",
                    "is_unscheduled_stop",
                    "has_exception_only_service",
                    "has_any_bus_service",
                    "has_daily_bus_service",
                    "route_modes_json",
                    "source_reason_codes_json",
                    "reality_reason_codes_json",
                    "lat",
                    "lon",
                    "created_at",
                ],
            )?,
            counts: BTreeMap::new(),
        })
    }

    fn bump(&mut self, key: &str) {
        *self.counts.entry(key.to_string()).or_insert(0) += 1;
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.service_classification.flush()?;
        self.stop_summary.flush()?;
        self.gtfs_stop_reality.flush()?;
        Ok(())
    }

    fn write_service_classification(
        &mut self,
        row: &ServiceClassification,
        settings: &RunSettings,
    ) -> Result<(), Box<dyn Error>> {
        self.service_classification.write_record([
            settings.reality_fingerprint.as_str(),
            row.feed_id.as_str(),
            row.service_id.as_str(),
            row.school_only_state.as_str(),
            &json_string(&row.route_ids)?,
            &json_string(&row.route_modes)?,
            &json_string(&row.reason_codes)?,
            &serde_json::to_string(&row.time_bucket_counts)?,
            settings.created_at.as_str(),
        ])?;
        self.bump("service_classification");
        Ok(())
    }

    fn write_stop_summary(
        &mut self,
        row: &StopServiceSummary,
        settings: &RunSettings,
    ) -> Result<(), Box<dyn Error>> {
        self.stop_summary.write_record([
            settings.reality_fingerprint.as_str(),
            row.feed_id.as_str(),
            row.stop_id.as_str(),
            &row.public_departures_7d.to_string(),
            &row.public_departures_30d.to_string(),
            &row.school_only_departures_30d.to_string(),
            &float_string(row.weekday_morning_peak_deps),
            &float_string(row.weekday_evening_peak_deps),
            &float_string(row.weekday_offpeak_deps),
            &float_string(row.saturday_deps),
            &float_string(row.sunday_deps),
            &float_string(row.friday_evening_deps),
            &row.transport_score_units.to_string(),
            &float_string(row.bus_daytime_deps),
            &optional_float_string(row.bus_daytime_headway_min),
            row.bus_frequency_tier.as_deref().unwrap_or(""),
            &row.bus_frequency_score_units.to_string(),
            &optional_date_string(row.last_public_service_date),
            &optional_date_string(row.last_any_service_date),
            row.bus_active_days_mask_7d.as_deref().unwrap_or(""),
            row.bus_service_subtier.as_deref().unwrap_or(""),
            bool_string(row.is_unscheduled_stop),
            bool_string(row.has_exception_only_service),
            bool_string(row.has_any_bus_service),
            bool_string(row.has_daily_bus_service),
            &json_string(&row.route_modes)?,
            &json_string(&row.route_ids)?,
            &json_string(&row.reason_codes)?,
            settings.created_at.as_str(),
        ])?;
        self.bump("gtfs_stop_service_summary");
        Ok(())
    }

    fn write_gtfs_stop_reality(
        &mut self,
        row: &GtfsStopReality,
        settings: &RunSettings,
    ) -> Result<(), Box<dyn Error>> {
        self.gtfs_stop_reality.write_record([
            settings.reality_fingerprint.as_str(),
            settings.import_fingerprint.as_str(),
            row.source_ref.as_str(),
            row.stop_name.as_deref().unwrap_or(""),
            row.feed_id.as_str(),
            row.stop_id.as_str(),
            row.source_status.as_str(),
            row.reality_status.as_str(),
            row.school_only_state.as_str(),
            &row.public_departures_7d.to_string(),
            &row.public_departures_30d.to_string(),
            &row.school_only_departures_30d.to_string(),
            &float_string(row.weekday_morning_peak_deps),
            &float_string(row.weekday_evening_peak_deps),
            &float_string(row.weekday_offpeak_deps),
            &float_string(row.saturday_deps),
            &float_string(row.sunday_deps),
            &float_string(row.friday_evening_deps),
            &row.transport_score_units.to_string(),
            &float_string(row.bus_daytime_deps),
            &optional_float_string(row.bus_daytime_headway_min),
            row.bus_frequency_tier.as_deref().unwrap_or(""),
            &row.bus_frequency_score_units.to_string(),
            &optional_date_string(row.last_public_service_date),
            &optional_date_string(row.last_any_service_date),
            row.bus_active_days_mask_7d.as_deref().unwrap_or(""),
            row.bus_service_subtier.as_deref().unwrap_or(""),
            bool_string(row.is_unscheduled_stop),
            bool_string(row.has_exception_only_service),
            bool_string(row.has_any_bus_service),
            bool_string(row.has_daily_bus_service),
            &json_string(&row.route_modes)?,
            &json_string(&row.source_reason_codes)?,
            &json_string(&row.reality_reason_codes)?,
            &float_string(row.lat),
            &float_string(row.lon),
            settings.created_at.as_str(),
        ])?;
        self.bump("gtfs_stop_reality");
        Ok(())
    }
}

fn new_csv_writer(path: &Path, headers: &[&str]) -> Result<CsvWriter, Box<dyn Error>> {
    CsvWriter::new(path, headers)
}

fn write_csv_field(writer: &mut impl Write, value: &str) -> Result<(), Box<dyn Error>> {
    let needs_quotes = value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r');
    if !needs_quotes {
        writer.write_all(value.as_bytes())?;
        return Ok(());
    }
    writer.write_all(b"\"")?;
    for ch in value.chars() {
        if ch == '"' {
            writer.write_all(b"\"\"")?;
        } else {
            write!(writer, "{ch}")?;
        }
    }
    writer.write_all(b"\"")?;
    Ok(())
}

fn json_string<T: Serialize>(value: &T) -> Result<String, Box<dyn Error>> {
    Ok(serde_json::to_string(value)?)
}

fn float_string(value: f64) -> String {
    let formatted = format!("{value:.8}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn bool_string(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn optional_i32_string(value: Option<i32>) -> String {
    value.map(|number| number.to_string()).unwrap_or_default()
}

fn optional_float_string(value: Option<f64>) -> String {
    value.map(float_string).unwrap_or_default()
}

fn optional_date_string(value: Option<Date>) -> String {
    value
        .and_then(|date| date.format(&ISO_DATE_FORMAT).ok())
        .unwrap_or_default()
}

fn parse_iso_date(value: &str, field_name: &str) -> Result<Date, Box<dyn Error>> {
    Date::parse(value, &ISO_DATE_FORMAT)
        .map_err(|err| format!("invalid {field_name} date '{value}': {err}").into())
}

fn parse_gtfs_date(value: &str) -> Result<Date, Box<dyn Error>> {
    Date::parse(value, &GTFS_DATE_FORMAT)
        .map_err(|err| format!("invalid GTFS date '{value}': {err}").into())
}

fn normalize_name(value: Option<&str>) -> String {
    let normalized: String = value
        .unwrap_or("")
        .chars()
        .flat_map(|ch| ch.to_lowercase())
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect();
    normalized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn school_tokens(value: Option<&str>, settings: &RunSettings) -> BTreeSet<String> {
    normalize_name(value)
        .split_whitespace()
        .filter(|token| settings.school_keywords.contains(*token))
        .map(str::to_string)
        .collect()
}

fn route_mode(route_type: Option<i32>) -> &'static str {
    match route_type.unwrap_or(-1) {
        0 => "tram",
        1 => "subway",
        2 => "rail",
        3 => "bus",
        4 => "ferry",
        5 => "cable_tram",
        6 => "aerial_lift",
        7 => "funicular",
        11 => "trolleybus",
        12 => "monorail",
        _ => "unknown",
    }
}

fn weekday_mask_for_indexes(weekdays: &BTreeSet<u8>) -> String {
    (0_u8..7)
        .map(|weekday| if weekdays.contains(&weekday) { '1' } else { '0' })
        .collect()
}

fn calendar_weekday_indexes(calendar: &CalendarService) -> BTreeSet<u8> {
    [
        calendar.monday,
        calendar.tuesday,
        calendar.wednesday,
        calendar.thursday,
        calendar.friday,
        calendar.saturday,
        calendar.sunday,
    ]
    .into_iter()
    .enumerate()
    .filter_map(|(weekday, flag)| if flag == 1 { Some(weekday as u8) } else { None })
    .collect()
}

fn is_boarding_stop(stop_info: &StopInfo) -> bool {
    matches!(stop_info.location_type, None | Some(0))
}

fn bus_subtier_for_mask(mask: &str) -> Option<&'static str> {
    match mask {
        "" | "0000000" => None,
        "1111111" => Some("mon_sun"),
        "1111110" => Some("mon_sat"),
        "0111111" => Some("tue_sun"),
        "1111100" => Some("weekdays_only"),
        "0000011" => Some("weekends_only"),
        _ if mask.chars().filter(|ch| *ch == '1').count() == 1 => Some("single_day_only"),
        _ => Some("partial_week"),
    }
}

fn time_bucket(seconds: Option<i32>, settings: &RunSettings) -> &'static str {
    let Some(total_seconds) = seconds else {
        return "offpeak";
    };
    let hour = ((total_seconds / 3600) % 24 + 24) % 24;
    if hour >= i32::from(settings.school_am_start_hour) && hour < i32::from(settings.school_am_end_hour) {
        "morning"
    } else if hour >= i32::from(settings.school_pm_start_hour) && hour < i32::from(settings.school_pm_end_hour) {
        "afternoon"
    } else {
        "offpeak"
    }
}

fn parse_hhmmss_to_seconds(value: Option<&str>) -> Result<Option<i32>, Box<dyn Error>> {
    let Some(text) = value.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    let parts: Vec<_> = text.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("invalid time '{text}'").into());
    }
    let hours: i32 = parts[0]
        .parse()
        .map_err(|_| format!("invalid time hours in '{text}'"))?;
    let minutes: i32 = parts[1]
        .parse()
        .map_err(|_| format!("invalid time minutes in '{text}'"))?;
    let seconds: i32 = parts[2]
        .parse()
        .map_err(|_| format!("invalid time seconds in '{text}'"))?;
    Ok(Some((hours * 3600) + (minutes * 60) + seconds))
}

fn max_optional_date(current: Option<Date>, candidate: Option<Date>) -> Option<Date> {
    match (current, candidate) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if matches!(chars.peek(), Some('"')) {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                fields.push(field);
                field = String::new();
            }
            '\r' => {}
            _ => field.push(ch),
        }
    }
    fields.push(field);
    fields
}

fn required_member_map<R: Read + Seek>(
    archive: &ZipArchive<R>,
) -> Result<HashMap<String, usize>, Box<dyn Error>> {
    let mut members = HashMap::new();
    for index in 0..archive.len() {
        let Some(name) = archive.file_names().nth(index) else {
            continue;
        };
        members.insert(name.to_string(), index);
    }
    for required in REQUIRED_GTFS_FILENAMES {
        if !members.contains_key(required) {
            return Err(format!("missing required GTFS file: {required}").into());
        }
    }
    if !members.contains_key("calendar.txt") && !members.contains_key("calendar_dates.txt") {
        return Err(
            "missing required GTFS service calendar file: calendar.txt or calendar_dates.txt"
                .into(),
        );
    }
    Ok(members)
}

fn for_each_zip_csv_row<R, F>(
    archive: &mut ZipArchive<R>,
    member_index: &usize,
    mut handler: F,
) -> Result<(), Box<dyn Error>>
where
    R: Read + Seek,
    F: FnMut(&HashMap<String, usize>, &[String]) -> Result<(), Box<dyn Error>>,
{
    let member = archive.by_index(*member_index)?;
    let reader = BufReader::new(member);
    let mut lines = reader.lines();

    let Some(header_line) = lines.next() else {
        return Ok(());
    };
    let header_row = parse_csv_line(&header_line?);
    let headers: HashMap<String, usize> = header_row
        .iter()
        .enumerate()
        .map(|(index, name)| (name.trim().to_string(), index))
        .collect();

    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record = parse_csv_line(&line);
        if record.iter().all(|f| f.trim().is_empty()) {
            continue;
        }
        handler(&headers, &record)?;
    }
    Ok(())
}

fn field_value<'a>(
    record: &'a [String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<Option<&'a str>, Box<dyn Error>> {
    let Some(index) = headers.get(field_name) else {
        return Err(format!("missing column '{field_name}' in {source_name}").into());
    };
    Ok(record.get(*index).map(String::as_str))
}

fn required_text(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<String, Box<dyn Error>> {
    let value = field_value(record, headers, field_name, source_name)?
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| format!("missing {field_name} in {source_name}"))?;
    Ok(value.to_string())
}

fn optional_text(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
) -> Option<String> {
    headers
        .get(field_name)
        .and_then(|index| record.get(*index))
        .map(String::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn required_float(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<f64, Box<dyn Error>> {
    required_text(record, headers, field_name, source_name)?
        .parse()
        .map_err(|_| format!("invalid float for {field_name} in {source_name}").into())
}

fn required_int(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<i32, Box<dyn Error>> {
    required_text(record, headers, field_name, source_name)?
        .parse()
        .map_err(|_| format!("invalid integer for {field_name} in {source_name}").into())
}

fn optional_int(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<Option<i32>, Box<dyn Error>> {
    let Some(value) = optional_text(record, headers, field_name) else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|_| format!("invalid integer for {field_name} in {source_name}").into())
}

fn required_gtfs_date(
    record: &[String],
    headers: &HashMap<String, usize>,
    field_name: &str,
    source_name: &str,
) -> Result<Date, Box<dyn Error>> {
    let value = required_text(record, headers, field_name, source_name)?;
    parse_gtfs_date(&value)
}

fn parse_gtfs_feed(
    feed: &FeedInput,
    settings: &RunSettings,
    raw_dir: &Path,
) -> Result<(FeedDataset, FeedRunSummary), Box<dyn Error>> {
    let mut archive = ZipArchive::new(File::open(&feed.zip_path)?)?;
    let members = required_member_map(&archive)?;
    let mut writers = FeedRawWriters::new(raw_dir)?;
    let mut dataset = FeedDataset {
        feed_id: feed.feed_id.clone(),
        stops: BTreeMap::new(),
        routes: BTreeMap::new(),
        trips: BTreeMap::new(),
        calendar_services: BTreeMap::new(),
        calendar_dates: Vec::new(),
        stop_service_occurrences: HashMap::new(),
        stop_service_time_occurrences: HashMap::new(),
        service_time_buckets: HashMap::new(),
        service_route_ids: HashMap::new(),
        service_route_modes: HashMap::new(),
        service_keywords: HashMap::new(),
    };

    {
        for_each_zip_csv_row(
            &mut archive,
            members.get("stops.txt").unwrap(),
            |headers, record| {
                let stop_id = required_text(&record, &headers, "stop_id", "stops.txt")?;
                let stop_name = required_text(&record, &headers, "stop_name", "stops.txt")?;
                let stop_lat = required_float(&record, &headers, "stop_lat", "stops.txt")?;
                let stop_lon = required_float(&record, &headers, "stop_lon", "stops.txt")?;
                let stop_info = StopInfo {
                    stop_code: optional_text(&record, &headers, "stop_code"),
                    stop_name: stop_name.clone(),
                    stop_desc: optional_text(&record, &headers, "stop_desc"),
                    stop_lat,
                    stop_lon,
                    parent_station: optional_text(&record, &headers, "parent_station"),
                    zone_id: optional_text(&record, &headers, "zone_id"),
                    location_type: optional_int(&record, &headers, "location_type", "stops.txt")?,
                    wheelchair_boarding: optional_int(
                        &record,
                        &headers,
                        "wheelchair_boarding",
                        "stops.txt",
                    )?,
                    platform_code: optional_text(&record, &headers, "platform_code"),
                };
                dataset.stops.insert(stop_id.clone(), stop_info.clone());
                writers.stops.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    stop_id.as_str(),
                    stop_info.stop_code.as_deref().unwrap_or(""),
                    stop_name.as_str(),
                    stop_info.stop_desc.as_deref().unwrap_or(""),
                    &float_string(stop_lat),
                    &float_string(stop_lon),
                    stop_info.parent_station.as_deref().unwrap_or(""),
                    stop_info.zone_id.as_deref().unwrap_or(""),
                    &optional_i32_string(stop_info.location_type),
                    &optional_i32_string(stop_info.wheelchair_boarding),
                    stop_info.platform_code.as_deref().unwrap_or(""),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("stops");
                Ok(())
            },
        )?;
    }

    {
        for_each_zip_csv_row(
            &mut archive,
            members.get("routes.txt").unwrap(),
            |headers, record| {
                let route_id = required_text(&record, &headers, "route_id", "routes.txt")?;
                let route_info = RouteInfo {
                    agency_id: optional_text(&record, &headers, "agency_id"),
                    route_short_name: optional_text(&record, &headers, "route_short_name"),
                    route_long_name: optional_text(&record, &headers, "route_long_name"),
                    route_desc: optional_text(&record, &headers, "route_desc"),
                    route_type: optional_int(&record, &headers, "route_type", "routes.txt")?,
                    route_url: optional_text(&record, &headers, "route_url"),
                    route_color: optional_text(&record, &headers, "route_color"),
                    route_text_color: optional_text(&record, &headers, "route_text_color"),
                };
                dataset.routes.insert(route_id.clone(), route_info.clone());
                writers.routes.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    route_id.as_str(),
                    route_info.agency_id.as_deref().unwrap_or(""),
                    route_info.route_short_name.as_deref().unwrap_or(""),
                    route_info.route_long_name.as_deref().unwrap_or(""),
                    route_info.route_desc.as_deref().unwrap_or(""),
                    &optional_i32_string(route_info.route_type),
                    route_info.route_url.as_deref().unwrap_or(""),
                    route_info.route_color.as_deref().unwrap_or(""),
                    route_info.route_text_color.as_deref().unwrap_or(""),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("routes");
                Ok(())
            },
        )?;
    }

    {
        for_each_zip_csv_row(
            &mut archive,
            members.get("trips.txt").unwrap(),
            |headers, record| {
                let trip_id = required_text(&record, &headers, "trip_id", "trips.txt")?;
                let route_id = required_text(&record, &headers, "route_id", "trips.txt")?;
                let service_id = required_text(&record, &headers, "service_id", "trips.txt")?;
                let route_info = dataset.routes.get(&route_id);
                let trip_info = TripInfo {
                    route_id: route_id.clone(),
                    service_id: service_id.clone(),
                    trip_headsign: optional_text(&record, &headers, "trip_headsign"),
                    trip_short_name: optional_text(&record, &headers, "trip_short_name"),
                    direction_id: optional_int(&record, &headers, "direction_id", "trips.txt")?,
                    block_id: optional_text(&record, &headers, "block_id"),
                    shape_id: optional_text(&record, &headers, "shape_id"),
                    mode: route_mode(route_info.and_then(|route| route.route_type)).to_string(),
                };
                dataset.trips.insert(trip_id.clone(), trip_info.clone());
                writers.trips.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    route_id.as_str(),
                    service_id.as_str(),
                    trip_id.as_str(),
                    trip_info.trip_headsign.as_deref().unwrap_or(""),
                    trip_info.trip_short_name.as_deref().unwrap_or(""),
                    &optional_i32_string(trip_info.direction_id),
                    trip_info.block_id.as_deref().unwrap_or(""),
                    trip_info.shape_id.as_deref().unwrap_or(""),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("trips");

                if let Some(route_info) = route_info {
                    dataset
                        .service_route_ids
                        .entry(service_id.clone())
                        .or_default()
                        .insert(route_id.clone());
                    dataset
                        .service_route_modes
                        .entry(service_id.clone())
                        .or_default()
                        .insert(route_mode(route_info.route_type).to_string());
                    let route_text = [
                        route_info.route_short_name.as_deref(),
                        route_info.route_long_name.as_deref(),
                        route_info.route_desc.as_deref(),
                        trip_info.trip_headsign.as_deref(),
                        trip_info.trip_short_name.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" ");
                    let keywords = school_tokens(Some(route_text.as_str()), settings);
                    if !keywords.is_empty() {
                        dataset
                            .service_keywords
                            .entry(service_id.clone())
                            .or_default()
                            .extend(keywords);
                    }
                }
                Ok(())
            },
        )?;
    }

    if let Some(calendar_member_index) = members.get("calendar.txt") {
        for_each_zip_csv_row(
            &mut archive,
            calendar_member_index,
            |headers, record| {
                let service_id = required_text(&record, &headers, "service_id", "calendar.txt")?;
                let calendar = CalendarService {
                    monday: required_int(&record, &headers, "monday", "calendar.txt")?,
                    tuesday: required_int(&record, &headers, "tuesday", "calendar.txt")?,
                    wednesday: required_int(&record, &headers, "wednesday", "calendar.txt")?,
                    thursday: required_int(&record, &headers, "thursday", "calendar.txt")?,
                    friday: required_int(&record, &headers, "friday", "calendar.txt")?,
                    saturday: required_int(&record, &headers, "saturday", "calendar.txt")?,
                    sunday: required_int(&record, &headers, "sunday", "calendar.txt")?,
                    start_date: required_gtfs_date(
                        &record,
                        &headers,
                        "start_date",
                        "calendar.txt",
                    )?,
                    end_date: required_gtfs_date(&record, &headers, "end_date", "calendar.txt")?,
                };
                dataset
                    .calendar_services
                    .insert(service_id.clone(), calendar.clone());
                writers.calendar_services.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    service_id.as_str(),
                    &calendar.monday.to_string(),
                    &calendar.tuesday.to_string(),
                    &calendar.wednesday.to_string(),
                    &calendar.thursday.to_string(),
                    &calendar.friday.to_string(),
                    &calendar.saturday.to_string(),
                    &calendar.sunday.to_string(),
                    &calendar.start_date.to_string(),
                    &calendar.end_date.to_string(),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("calendar_services");
                Ok(())
            },
        )?;
    }

    if let Some(calendar_dates_member_index) = members.get("calendar_dates.txt") {
        for_each_zip_csv_row(
            &mut archive,
            calendar_dates_member_index,
            |headers, record| {
                let exception = CalendarDateException {
                    service_id: required_text(
                        &record,
                        &headers,
                        "service_id",
                        "calendar_dates.txt",
                    )?,
                    service_date: required_gtfs_date(
                        &record,
                        &headers,
                        "date",
                        "calendar_dates.txt",
                    )?,
                    exception_type: required_int(
                        &record,
                        &headers,
                        "exception_type",
                        "calendar_dates.txt",
                    )?,
                };
                dataset.calendar_dates.push(exception.clone());
                writers.calendar_dates.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    exception.service_id.as_str(),
                    &exception.service_date.to_string(),
                    &exception.exception_type.to_string(),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("calendar_dates");
                Ok(())
            },
        )?;
    }

    {
        for_each_zip_csv_row(
            &mut archive,
            members.get("stop_times.txt").unwrap(),
            |headers, record| {
                let trip_id = required_text(&record, &headers, "trip_id", "stop_times.txt")?;
                let trip_info = dataset.trips.get(&trip_id).ok_or_else(|| {
                    format!(
                        "GTFS stop_times.txt references unknown trip_id {:?} in feed {}.",
                        trip_id, feed.feed_id
                    )
                })?;
                let stop_id = required_text(&record, &headers, "stop_id", "stop_times.txt")?;
                if !dataset.stops.contains_key(&stop_id) {
                    return Err(format!(
                        "GTFS stop_times.txt references unknown stop_id {:?} in feed {}.",
                        stop_id, feed.feed_id
                    )
                    .into());
                }
                let departure_seconds = parse_hhmmss_to_seconds(
                    optional_text(&record, &headers, "departure_time").as_deref(),
                )?;
                let arrival_seconds = parse_hhmmss_to_seconds(
                    optional_text(&record, &headers, "arrival_time").as_deref(),
                )?;
                let stop_sequence =
                    optional_int(&record, &headers, "stop_sequence", "stop_times.txt")?
                        .ok_or_else(|| {
                            "GTFS stop_times.txt is missing required field 'stop_sequence'."
                                .to_string()
                        })?;

                let occurrence_key = (
                    stop_id.clone(),
                    trip_info.service_id.clone(),
                    trip_info.route_id.clone(),
                    trip_info.mode.clone(),
                );
                *dataset
                    .stop_service_occurrences
                    .entry(occurrence_key)
                    .or_insert(0) += 1;

                let event_seconds = departure_seconds.or(arrival_seconds);
                let timed_occurrence_key = (
                    stop_id.clone(),
                    trip_info.service_id.clone(),
                    trip_info.route_id.clone(),
                    trip_info.mode.clone(),
                    event_seconds,
                );
                *dataset
                    .stop_service_time_occurrences
                    .entry(timed_occurrence_key)
                    .or_insert(0) += 1;

                let bucket = time_bucket(event_seconds, settings);
                let bucket_counts = dataset
                    .service_time_buckets
                    .entry(trip_info.service_id.clone())
                    .or_default();
                match bucket {
                    "morning" => bucket_counts.morning += 1,
                    "afternoon" => bucket_counts.afternoon += 1,
                    _ => bucket_counts.offpeak += 1,
                }

                writers.stop_times.write_record([
                    feed.feed_fingerprint.as_str(),
                    feed.feed_id.as_str(),
                    trip_id.as_str(),
                    &optional_i32_string(arrival_seconds.map(|value| value as i32)),
                    &optional_i32_string(departure_seconds.map(|value| value as i32)),
                    stop_id.as_str(),
                    &stop_sequence.to_string(),
                    &optional_i32_string(optional_int(
                        &record,
                        &headers,
                        "pickup_type",
                        "stop_times.txt",
                    )?),
                    &optional_i32_string(optional_int(
                        &record,
                        &headers,
                        "drop_off_type",
                        "stop_times.txt",
                    )?),
                    settings.created_at.as_str(),
                ])?;
                writers.bump("stop_times");
                Ok(())
            },
        )?;
    }

    writers.flush()?;
    Ok((
        dataset,
        FeedRunSummary {
            feed_id: feed.feed_id.clone(),
            label: feed.label.clone(),
            feed_fingerprint: feed.feed_fingerprint.clone(),
            source_path: feed.zip_path.display().to_string(),
            raw_counts: writers.counts,
        },
    ))
}

fn expand_service_windows(
    dataset: &FeedDataset,
    settings: &RunSettings,
) -> BTreeMap<String, ServiceWindow> {
    let mut exception_map: HashMap<String, BTreeMap<Date, i32>> = HashMap::new();
    for row in &dataset.calendar_dates {
        exception_map
            .entry(row.service_id.clone())
            .or_default()
            .insert(row.service_date, row.exception_type);
    }

    let analysis_start =
        settings.analysis_date - Duration::days((settings.analysis_window_days as i64) - 1);
    let analysis_end = settings.analysis_date + Duration::days(settings.lookahead_days as i64);
    let desert_start =
        settings.analysis_date - Duration::days((settings.service_desert_window_days as i64) - 1);
    let mut service_ids = BTreeSet::new();
    service_ids.extend(dataset.calendar_services.keys().cloned());
    service_ids.extend(exception_map.keys().cloned());

    let mut service_windows = BTreeMap::new();
    for service_id in service_ids {
        let calendar = dataset.calendar_services.get(&service_id);
        let mut active_dates_30d = BTreeSet::new();
        let mut active_dates_7d = BTreeSet::new();
        let mut weekday_dates = 0_u32;
        let mut weekend_dates = 0_u32;

        if let Some(calendar) = calendar {
            let mut current_date = if calendar.start_date > analysis_start {
                calendar.start_date
            } else {
                analysis_start
            };
            let end_date = if calendar.end_date < analysis_end {
                calendar.end_date
            } else {
                analysis_end
            };
            while current_date <= end_date {
                let weekday_flag = match current_date.weekday().number_days_from_monday() {
                    0 => calendar.monday,
                    1 => calendar.tuesday,
                    2 => calendar.wednesday,
                    3 => calendar.thursday,
                    4 => calendar.friday,
                    5 => calendar.saturday,
                    _ => calendar.sunday,
                };
                if weekday_flag == 1 {
                    active_dates_30d.insert(current_date);
                    if current_date >= desert_start {
                        active_dates_7d.insert(current_date);
                    }
                }
                current_date = current_date.next_day().unwrap();
            }
        }

        if let Some(exceptions) = exception_map.get(&service_id) {
            for (exception_date, exception_type) in exceptions {
                if *exception_date < analysis_start || *exception_date > analysis_end {
                    continue;
                }
                match *exception_type {
                    1 => {
                        active_dates_30d.insert(*exception_date);
                        if *exception_date >= desert_start {
                            active_dates_7d.insert(*exception_date);
                        }
                    }
                    2 => {
                        active_dates_30d.remove(exception_date);
                        active_dates_7d.remove(exception_date);
                    }
                    _ => {}
                }
            }
        }

        for active_date in &active_dates_30d {
            if active_date.weekday().number_days_from_monday() >= 5 {
                weekend_dates += 1;
            } else {
                weekday_dates += 1;
            }
        }

        service_windows.insert(
            service_id,
            ServiceWindow {
                dates_30d: active_dates_30d.into_iter().collect(),
                dates_7d: active_dates_7d.into_iter().collect(),
                weekday_dates,
                weekend_dates,
            },
        );
    }
    service_windows
}

fn classify_services(
    dataset: &FeedDataset,
    _reality_fingerprint: &str,
    service_windows: &BTreeMap<String, ServiceWindow>,
) -> BTreeMap<String, ServiceClassification> {
    let mut service_ids = BTreeSet::new();
    service_ids.extend(dataset.service_route_ids.keys().cloned());
    service_ids.extend(dataset.service_time_buckets.keys().cloned());
    service_ids.extend(service_windows.keys().cloned());

    let mut classifications = BTreeMap::new();
    for service_id in service_ids {
        let window = service_windows.get(&service_id);
        let bucket_counts = dataset
            .service_time_buckets
            .get(&service_id)
            .cloned()
            .unwrap_or_default();
        let total_events = bucket_counts.morning + bucket_counts.afternoon + bucket_counts.offpeak;
        let school_bucket_events = bucket_counts.morning + bucket_counts.afternoon;
        let school_bucket_share = if total_events > 0 {
            school_bucket_events as f64 / total_events as f64
        } else {
            0.0
        };
        let weekday_dates = window.map(|row| row.weekday_dates).unwrap_or(0);
        let weekend_dates = window.map(|row| row.weekend_dates).unwrap_or(0);
        let has_keyword = dataset
            .service_keywords
            .get(&service_id)
            .map(|keywords| !keywords.is_empty())
            .unwrap_or(false);
        let route_ids = dataset
            .service_route_ids
            .get(&service_id)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_else(Vec::new);
        let route_modes = dataset
            .service_route_modes
            .get(&service_id)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_else(Vec::new);

        let mut reason_codes = BTreeSet::new();
        if weekday_dates > 0 {
            reason_codes.insert("weekday_service_present".to_string());
        }
        if weekend_dates > 0 {
            reason_codes.insert("weekend_service_present".to_string());
        }
        if school_bucket_share >= 0.9 && total_events > 0 {
            reason_codes.insert("school_hour_concentration".to_string());
        }
        if has_keyword {
            reason_codes.insert("school_keyword".to_string());
        }

        let school_only_state =
            if weekday_dates > 0 && weekend_dates == 0 && school_bucket_share >= 0.9 {
                if has_keyword {
                    "yes"
                } else {
                    "unknown"
                }
            } else {
                "no"
            };

        classifications.insert(
            service_id.clone(),
            ServiceClassification {
                feed_id: dataset.feed_id.clone(),
                service_id,
                school_only_state: school_only_state.to_string(),
                route_ids,
                route_modes,
                reason_codes: reason_codes.into_iter().collect(),
                time_bucket_counts: bucket_counts,
            },
        );
    }
    classifications
}

fn hour_window_contains(seconds: Option<i32>, start_hour: u8, end_hour: u8) -> bool {
    let Some(seconds) = seconds else {
        return false;
    };
    let start_seconds = i32::from(start_hour) * 3600;
    let mut end_seconds = i32::from(end_hour) * 3600;
    if end_seconds <= start_seconds {
        end_seconds += 24 * 3600;
    }
    seconds >= start_seconds && seconds < end_seconds
}

fn is_weekday_morning_peak(seconds: Option<i32>, settings: &RunSettings) -> bool {
    hour_window_contains(
        seconds,
        settings.commute_am_start_hour,
        settings.commute_am_end_hour,
    )
}

fn is_weekday_evening_peak(seconds: Option<i32>, settings: &RunSettings) -> bool {
    hour_window_contains(
        seconds,
        settings.commute_pm_start_hour,
        settings.commute_pm_end_hour,
    )
}

fn is_friday_evening(seconds: Option<i32>, settings: &RunSettings) -> bool {
    hour_window_contains(
        seconds,
        settings.friday_evening_start_hour,
        settings.friday_evening_end_hour,
    )
}

fn is_bus_daytime(seconds: Option<i32>, settings: &RunSettings) -> bool {
    hour_window_contains(
        seconds,
        settings.bus_daytime_start_hour,
        settings.bus_daytime_end_hour,
    )
}

fn average_departures(total: u32, dates: &BTreeSet<Date>) -> f64 {
    if dates.is_empty() {
        0.0
    } else {
        (total as f64) / (dates.len() as f64)
    }
}

fn bus_daytime_window_minutes(settings: &RunSettings) -> f64 {
    let start = u32::from(settings.bus_daytime_start_hour) * 60;
    let mut end = u32::from(settings.bus_daytime_end_hour) * 60;
    if end <= start {
        end += 24 * 60;
    }
    (end - start) as f64
}

fn bus_frequency_tier_from_headway(headway_minutes: Option<f64>) -> (Option<String>, u32) {
    let Some(headway) = headway_minutes else {
        return (None, 0);
    };
    if !headway.is_finite() || headway <= 0.0 {
        return (None, 0);
    }
    let tier = if headway <= 15.0 {
        "frequent"
    } else if headway <= 30.0 {
        "moderate"
    } else if headway <= 60.0 {
        "low_frequency"
    } else if headway <= 120.0 {
        "very_low_frequency"
    } else {
        "token_skeletal"
    };
    let units = match tier {
        "frequent" => 5,
        "moderate" => 4,
        "low_frequency" => 3,
        "very_low_frequency" => 2,
        _ => 1,
    };
    (Some(tier.to_string()), units)
}

fn transport_score_units_from_frequency(
    weekday_morning_peak_deps: f64,
    weekday_evening_peak_deps: f64,
    weekday_offpeak_deps: f64,
    saturday_deps: f64,
    sunday_deps: f64,
    friday_evening_deps: f64,
    public_departures_30d: u32,
) -> u32 {
    if public_departures_30d == 0 {
        return 0;
    }
    let peak_am = (weekday_morning_peak_deps.max(0.0) / 16.0).min(1.0);
    let peak_pm = (weekday_evening_peak_deps.max(0.0) / 16.0).min(1.0);
    let commute = 0.6 * peak_am.min(peak_pm) + 0.4 * ((peak_am + peak_pm) / 2.0);
    let friday = (friday_evening_deps.max(0.0) / 24.0).min(1.0);
    let offpeak = (weekday_offpeak_deps.max(0.0) / 32.0).min(1.0);
    let weekend = (((saturday_deps.max(0.0) + sunday_deps.max(0.0)) / 2.0) / 24.0).min(1.0);
    let frequency = 0.60 * commute + 0.20 * friday + 0.10 * offpeak + 0.10 * weekend;
    ((frequency * 5.0).ceil() as u32).clamp(1, 5)
}

fn stop_service_time_occurrences(
    dataset: &FeedDataset,
) -> Vec<(String, String, String, String, Option<i32>, u32)> {
    if !dataset.stop_service_time_occurrences.is_empty() {
        return dataset
            .stop_service_time_occurrences
            .iter()
            .map(|((stop_id, service_id, route_id, mode, seconds), occurrences)| {
                (
                    stop_id.clone(),
                    service_id.clone(),
                    route_id.clone(),
                    mode.clone(),
                    *seconds,
                    *occurrences,
                )
            })
            .collect();
    }
    dataset
        .stop_service_occurrences
        .iter()
        .map(|((stop_id, service_id, route_id, mode), occurrences)| {
            (
                stop_id.clone(),
                service_id.clone(),
                route_id.clone(),
                mode.clone(),
                None,
                *occurrences,
            )
        })
        .collect()
}

fn summarize_gtfs_stops(
    dataset: &FeedDataset,
    settings: &RunSettings,
    _reality_fingerprint: &str,
    service_windows: &BTreeMap<String, ServiceWindow>,
    service_classifications: &BTreeMap<String, ServiceClassification>,
) -> Vec<StopServiceSummary> {
    #[derive(Default)]
    struct StopPayload {
        public_departures_7d: u32,
        public_departures_30d: u32,
        school_only_departures_30d: u32,
        last_public_service_date: Option<Date>,
        last_any_service_date: Option<Date>,
        route_modes: BTreeSet<String>,
        route_ids: BTreeSet<String>,
        reason_codes: BTreeSet<String>,
        base_weekly_bus_weekdays: BTreeSet<u8>,
        is_unscheduled_stop: bool,
        has_exception_only_service: bool,
        has_any_bus_service: bool,
        weekday_morning_peak_total: u32,
        weekday_evening_peak_total: u32,
        weekday_offpeak_total: u32,
        saturday_total: u32,
        sunday_total: u32,
        friday_evening_total: u32,
        bus_daytime_total: u32,
        weekday_dates: BTreeSet<Date>,
        saturday_dates: BTreeSet<Date>,
        sunday_dates: BTreeSet<Date>,
        friday_dates: BTreeSet<Date>,
        bus_daytime_dates: BTreeSet<Date>,
    }

    let exception_only_service_ids: BTreeSet<String> = dataset
        .calendar_dates
        .iter()
        .filter_map(|row| {
            if dataset.calendar_services.contains_key(&row.service_id) {
                None
            } else {
                Some(row.service_id.clone())
            }
        })
        .collect();
    let stop_ids_with_stop_times: BTreeSet<String> = dataset
        .stop_service_occurrences
        .keys()
        .map(|(stop_id, _, _, _)| stop_id.clone())
        .collect();
    let mut per_stop: BTreeMap<String, StopPayload> = BTreeMap::new();
    for (stop_id, service_id, route_id, mode, event_seconds, occurrences) in
        stop_service_time_occurrences(dataset)
    {
        let window = service_windows.get(&service_id);
        let dates_30d = window.map(|row| row.dates_30d.as_slice()).unwrap_or(&[]);
        let dates_7d = window.map(|row| row.dates_7d.as_slice()).unwrap_or(&[]);
        let classification = service_classifications.get(&service_id);
        let payload = per_stop.entry(stop_id.clone()).or_default();
        payload.route_modes.insert(mode.clone());
        payload.route_ids.insert(route_id.clone());
        if mode == "bus" {
            payload.has_any_bus_service = true;
            if let Some(calendar) = dataset.calendar_services.get(&service_id) {
                payload
                    .base_weekly_bus_weekdays
                    .extend(calendar_weekday_indexes(calendar));
            }
            payload.has_exception_only_service = payload.has_exception_only_service
                || exception_only_service_ids.contains(&service_id);
        }
        if let Some(last_date) = dates_30d.last().copied() {
            payload.last_any_service_date =
                max_optional_date(payload.last_any_service_date, Some(last_date));
        }

        if classification
            .map(|row| row.school_only_state.as_str() == "yes")
            .unwrap_or(false)
        {
            payload.school_only_departures_30d += occurrences * (dates_30d.len() as u32);
            payload
                .reason_codes
                .insert("school_only_service_present".to_string());
            continue;
        }

        payload.public_departures_7d += occurrences * (dates_7d.len() as u32);
        payload.public_departures_30d += occurrences * (dates_30d.len() as u32);
        for active_date in dates_30d {
            let weekday = active_date.weekday().number_days_from_monday();
            if weekday < 5 {
                payload.weekday_dates.insert(*active_date);
                if is_weekday_morning_peak(event_seconds, settings) {
                    payload.weekday_morning_peak_total += occurrences;
                } else if is_weekday_evening_peak(event_seconds, settings) {
                    payload.weekday_evening_peak_total += occurrences;
                } else {
                    payload.weekday_offpeak_total += occurrences;
                }
                if mode == "bus" && is_bus_daytime(event_seconds, settings) {
                    payload.bus_daytime_total += occurrences;
                    payload.bus_daytime_dates.insert(*active_date);
                }
            }
            if weekday == 5 {
                payload.saturday_dates.insert(*active_date);
                payload.saturday_total += occurrences;
            } else if weekday == 6 {
                payload.sunday_dates.insert(*active_date);
                payload.sunday_total += occurrences;
            }
            if weekday == 4 {
                payload.friday_dates.insert(*active_date);
                if is_friday_evening(event_seconds, settings) {
                    payload.friday_evening_total += occurrences;
                }
            }
        }
        if let Some(last_date) = dates_30d.last().copied() {
            payload.last_public_service_date =
                max_optional_date(payload.last_public_service_date, Some(last_date));
        }
    }

    for (stop_id, stop_info) in &dataset.stops {
        if !is_boarding_stop(stop_info) || stop_ids_with_stop_times.contains(stop_id) {
            continue;
        }
        let payload = per_stop.entry(stop_id.clone()).or_default();
        payload.is_unscheduled_stop = true;
        payload.reason_codes.insert("unscheduled_stop".to_string());
    }

    let mut summaries = Vec::new();
    for (stop_id, mut payload) in per_stop {
        let base_weekly_bus_mask = if payload.has_any_bus_service {
            Some(weekday_mask_for_indexes(&payload.base_weekly_bus_weekdays))
        } else {
            None
        };
        let bus_service_subtier = base_weekly_bus_mask
            .as_deref()
            .and_then(bus_subtier_for_mask)
            .map(str::to_string);
        let has_daily_bus_service = matches!(base_weekly_bus_mask.as_deref(), Some("1111111"));
        if payload.public_departures_30d > 0 {
            payload
                .reason_codes
                .insert("public_service_present".to_string());
        }
        if payload.school_only_departures_30d > 0 {
            payload
                .reason_codes
                .insert("school_only_service_present".to_string());
        }
        if payload.last_any_service_date.is_none() {
            payload.reason_codes.insert("no_service_window".to_string());
        }
        let weekday_morning_peak_deps =
            average_departures(payload.weekday_morning_peak_total, &payload.weekday_dates);
        let weekday_evening_peak_deps =
            average_departures(payload.weekday_evening_peak_total, &payload.weekday_dates);
        let weekday_offpeak_deps =
            average_departures(payload.weekday_offpeak_total, &payload.weekday_dates);
        let saturday_deps = average_departures(payload.saturday_total, &payload.saturday_dates);
        let sunday_deps = average_departures(payload.sunday_total, &payload.sunday_dates);
        let friday_evening_deps =
            average_departures(payload.friday_evening_total, &payload.friday_dates);
        let bus_daytime_deps =
            average_departures(payload.bus_daytime_total, &payload.bus_daytime_dates);
        let bus_daytime_headway_min = if bus_daytime_deps > 0.0 {
            Some(bus_daytime_window_minutes(settings) / bus_daytime_deps)
        } else {
            None
        };
        let (bus_frequency_tier, bus_frequency_score_units) =
            bus_frequency_tier_from_headway(bus_daytime_headway_min);
        let legacy_transport_score_units = transport_score_units_from_frequency(
            weekday_morning_peak_deps,
            weekday_evening_peak_deps,
            weekday_offpeak_deps,
            saturday_deps,
            sunday_deps,
            friday_evening_deps,
            payload.public_departures_30d,
        );
        let route_modes: Vec<String> = payload.route_modes.into_iter().collect();
        let transport_score_units = if route_modes.len() == 1 && route_modes[0] == "bus" {
            bus_frequency_score_units
        } else {
            legacy_transport_score_units
        };
        summaries.push(StopServiceSummary {
            feed_id: dataset.feed_id.clone(),
            stop_id,
            public_departures_7d: payload.public_departures_7d,
            public_departures_30d: payload.public_departures_30d,
            school_only_departures_30d: payload.school_only_departures_30d,
            weekday_morning_peak_deps,
            weekday_evening_peak_deps,
            weekday_offpeak_deps,
            saturday_deps,
            sunday_deps,
            friday_evening_deps,
            transport_score_units,
            bus_daytime_deps,
            bus_daytime_headway_min,
            bus_frequency_tier,
            bus_frequency_score_units,
            last_public_service_date: payload.last_public_service_date,
            last_any_service_date: payload.last_any_service_date,
            route_modes,
            route_ids: payload.route_ids.into_iter().collect(),
            reason_codes: payload.reason_codes.into_iter().collect(),
            // Legacy export field name; semantics are now the base weekly bus mask.
            bus_active_days_mask_7d: base_weekly_bus_mask,
            bus_service_subtier,
            is_unscheduled_stop: payload.is_unscheduled_stop,
            has_exception_only_service: payload.has_exception_only_service,
            has_any_bus_service: payload.has_any_bus_service,
            has_daily_bus_service,
        });
    }
    summaries
}

fn derive_gtfs_stop_reality(
    feed_datasets: &[FeedDataset],
    stop_summaries: &[StopServiceSummary],
    _reality_fingerprint: &str,
    _import_fingerprint: &str,
) -> Vec<GtfsStopReality> {
    let stop_info_by_key: HashMap<(String, String), StopInfo> = feed_datasets
        .iter()
        .flat_map(|dataset| {
            dataset.stops.iter().map(|(stop_id, stop_info)| {
                (
                    (dataset.feed_id.clone(), stop_id.clone()),
                    stop_info.clone(),
                )
            })
        })
        .collect();
    let mut summarized_children_by_parent: BTreeSet<(String, String)> = BTreeSet::new();
    for summary in stop_summaries {
        let key = (summary.feed_id.clone(), summary.stop_id.clone());
        let Some(stop_info) = stop_info_by_key.get(&key) else {
            continue;
        };
        let Some(parent_station) = stop_info.parent_station.as_ref() else {
            continue;
        };
        summarized_children_by_parent.insert((summary.feed_id.clone(), parent_station.clone()));
    }

    let mut reality_rows = Vec::new();
    for summary in stop_summaries {
        let key = (summary.feed_id.clone(), summary.stop_id.clone());
        let Some(stop_info) = stop_info_by_key.get(&key) else {
            continue;
        };
        if stop_info.location_type == Some(1) && summarized_children_by_parent.contains(&key) {
            continue;
        }

        let (reality_status, school_only_state, reason_code) = if summary.is_unscheduled_stop {
            (
                "inactive_confirmed",
                "no",
                "unscheduled_stop",
            )
        } else if summary.public_departures_30d > 0 {
            (
                "active_confirmed",
                "no",
                "public_departures_present",
            )
        } else if summary.school_only_departures_30d > 0 {
            (
                "school_only_confirmed",
                "yes",
                "school_only_departures_present",
            )
        } else {
            (
                "inactive_confirmed",
                "no",
                "zero_public_departures_window",
            )
        };
        let mut reality_reason_codes = BTreeSet::from([reason_code.to_string()]);
        reality_reason_codes.extend(summary.reason_codes.iter().cloned());

        reality_rows.push(GtfsStopReality {
            source_ref: format!("gtfs/{}/{}", summary.feed_id, summary.stop_id),
            stop_name: Some(stop_info.stop_name.clone()),
            feed_id: summary.feed_id.clone(),
            stop_id: summary.stop_id.clone(),
            source_status: "gtfs_direct".to_string(),
            reality_status: reality_status.to_string(),
            school_only_state: school_only_state.to_string(),
            public_departures_7d: summary.public_departures_7d,
            public_departures_30d: summary.public_departures_30d,
            school_only_departures_30d: summary.school_only_departures_30d,
            weekday_morning_peak_deps: summary.weekday_morning_peak_deps,
            weekday_evening_peak_deps: summary.weekday_evening_peak_deps,
            weekday_offpeak_deps: summary.weekday_offpeak_deps,
            saturday_deps: summary.saturday_deps,
            sunday_deps: summary.sunday_deps,
            friday_evening_deps: summary.friday_evening_deps,
            transport_score_units: summary.transport_score_units,
            bus_daytime_deps: summary.bus_daytime_deps,
            bus_daytime_headway_min: summary.bus_daytime_headway_min,
            bus_frequency_tier: summary.bus_frequency_tier.clone(),
            bus_frequency_score_units: summary.bus_frequency_score_units,
            last_public_service_date: summary.last_public_service_date,
            last_any_service_date: summary.last_any_service_date,
            route_modes: summary.route_modes.clone(),
            source_reason_codes: vec!["gtfs_direct_source".to_string()],
            reality_reason_codes: reality_reason_codes.into_iter().collect(),
            lat: stop_info.stop_lat,
            lon: stop_info.stop_lon,
            bus_active_days_mask_7d: summary.bus_active_days_mask_7d.clone(),
            bus_service_subtier: summary.bus_service_subtier.clone(),
            is_unscheduled_stop: summary.is_unscheduled_stop,
            has_exception_only_service: summary.has_exception_only_service,
            has_any_bus_service: summary.has_any_bus_service,
            has_daily_bus_service: summary.has_daily_bus_service,
        });
    }

    reality_rows.sort_by(|left, right| {
        (
            left.feed_id.as_str(),
            left.stop_name.as_deref().unwrap_or(""),
            left.stop_id.as_str(),
        )
            .cmp(&(
                right.feed_id.as_str(),
                right.stop_name.as_deref().unwrap_or(""),
                right.stop_id.as_str(),
            ))
    });
    reality_rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_with_stops(stops: Vec<(&str, StopInfo)>) -> FeedDataset {
        FeedDataset {
            feed_id: "nta".to_string(),
            stops: stops
                .into_iter()
                .map(|(stop_id, stop)| (stop_id.to_string(), stop))
                .collect(),
            routes: BTreeMap::new(),
            trips: BTreeMap::new(),
            calendar_services: BTreeMap::new(),
            calendar_dates: Vec::new(),
            stop_service_occurrences: HashMap::new(),
            stop_service_time_occurrences: HashMap::new(),
            service_time_buckets: HashMap::new(),
            service_route_ids: HashMap::new(),
            service_route_modes: HashMap::new(),
            service_keywords: HashMap::new(),
        }
    }

    fn stop_info(
        _stop_id: &str,
        stop_name: &str,
        lat: f64,
        lon: f64,
        parent_station: Option<&str>,
        location_type: Option<i32>,
        platform_code: Option<&str>,
    ) -> StopInfo {
        StopInfo {
            stop_code: None,
            stop_name: stop_name.to_string(),
            stop_desc: None,
            stop_lat: lat,
            stop_lon: lon,
            parent_station: parent_station.map(str::to_string),
            zone_id: None,
            location_type,
            wheelchair_boarding: None,
            platform_code: platform_code.map(str::to_string),
        }
    }

    fn stop_summary(
        stop_id: &str,
        public_departures_7d: u32,
        public_departures_30d: u32,
        school_only_departures_30d: u32,
    ) -> StopServiceSummary {
        StopServiceSummary {
            feed_id: "nta".to_string(),
            stop_id: stop_id.to_string(),
            public_departures_7d,
            public_departures_30d,
            school_only_departures_30d,
            weekday_morning_peak_deps: 0.0,
            weekday_evening_peak_deps: 0.0,
            weekday_offpeak_deps: 0.0,
            saturday_deps: 0.0,
            sunday_deps: 0.0,
            friday_evening_deps: 0.0,
            transport_score_units: if public_departures_30d > 0 { 1 } else { 0 },
            bus_daytime_deps: 0.0,
            bus_daytime_headway_min: None,
            bus_frequency_tier: None,
            bus_frequency_score_units: 0,
            last_public_service_date: Some(
                Date::from_calendar_date(2026, time::Month::April, 14).unwrap(),
            ),
            last_any_service_date: Some(
                Date::from_calendar_date(2026, time::Month::April, 14).unwrap(),
            ),
            bus_active_days_mask_7d: Some("1111111".to_string()),
            bus_service_subtier: Some("mon_sun".to_string()),
            is_unscheduled_stop: false,
            has_exception_only_service: false,
            has_any_bus_service: true,
            has_daily_bus_service: true,
            route_modes: vec!["bus".to_string()],
            route_ids: vec!["R1".to_string()],
            reason_codes: vec!["public_service_present".to_string()],
        }
    }

    fn default_settings(analysis_date: Date) -> RunSettings {
        RunSettings {
            analysis_date,
            analysis_window_days: 30,
            service_desert_window_days: 7,
            lookahead_days: 14,
            school_keywords: BTreeSet::new(),
            school_am_start_hour: 6,
            school_am_end_hour: 10,
            school_pm_start_hour: 13,
            school_pm_end_hour: 17,
            commute_am_start_hour: 4,
            commute_am_end_hour: 8,
            commute_pm_start_hour: 16,
            commute_pm_end_hour: 20,
            bus_daytime_start_hour: 6,
            bus_daytime_end_hour: 20,
            friday_evening_start_hour: 16,
            friday_evening_end_hour: 2,
            import_fingerprint: "import-123".to_string(),
            reality_fingerprint: "reality-123".to_string(),
            created_at: "2026-04-14T00:00:00+00:00".to_string(),
        }
    }

    fn single_bus_dataset(
        service_id: &str,
        weekday_flags: [i32; 7],
        location_type: Option<i32>,
    ) -> FeedDataset {
        let mut dataset = dataset_with_stops(vec![(
            "S1",
            stop_info("S1", "Main Street", 53.35, -6.26, None, location_type, None),
        )]);
        dataset.calendar_services.insert(
            service_id.to_string(),
            CalendarService {
                monday: weekday_flags[0],
                tuesday: weekday_flags[1],
                wednesday: weekday_flags[2],
                thursday: weekday_flags[3],
                friday: weekday_flags[4],
                saturday: weekday_flags[5],
                sunday: weekday_flags[6],
                start_date: Date::from_calendar_date(2026, time::Month::April, 1).unwrap(),
                end_date: Date::from_calendar_date(2026, time::Month::April, 30).unwrap(),
            },
        );
        dataset.stop_service_occurrences.insert(
            (
                "S1".to_string(),
                service_id.to_string(),
                "R1".to_string(),
                "bus".to_string(),
            ),
            1,
        );
        dataset.service_time_buckets.insert(
            service_id.to_string(),
            TimeBucketCounts {
                morning: 0,
                afternoon: 0,
                offpeak: 1,
            },
        );
        dataset.service_route_ids.insert(
            service_id.to_string(),
            BTreeSet::from(["R1".to_string()]),
        );
        dataset.service_route_modes.insert(
            service_id.to_string(),
            BTreeSet::from(["bus".to_string()]),
        );
        dataset
    }

    #[test]
    fn derive_gtfs_stop_reality_marks_public_service_active() {
        let dataset = dataset_with_stops(vec![(
            "S1",
            stop_info("S1", "Main Street", 53.35, -6.26, None, None, None),
        )]);
        let rows = derive_gtfs_stop_reality(
            &[dataset],
            &[stop_summary("S1", 7, 21, 0)],
            "reality-123",
            "import-123",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_ref, "gtfs/nta/S1");
        assert_eq!(rows[0].source_status, "gtfs_direct");
        assert_eq!(rows[0].reality_status, "active_confirmed");
        assert_eq!(rows[0].public_departures_30d, 21);
    }

    #[test]
    fn derive_gtfs_stop_reality_marks_zero_public_service_inactive() {
        let dataset = dataset_with_stops(vec![(
            "S1",
            stop_info("S1", "Main Street", 53.35, -6.26, None, None, None),
        )]);
        let rows = derive_gtfs_stop_reality(
            &[dataset],
            &[stop_summary("S1", 0, 0, 0)],
            "reality-123",
            "import-123",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reality_status, "inactive_confirmed");
        assert_eq!(rows[0].school_only_state, "no");
        assert_eq!(rows[0].public_departures_7d, 0);
    }

    #[test]
    fn derive_gtfs_stop_reality_marks_school_only_rows() {
        let dataset = dataset_with_stops(vec![(
            "S1",
            stop_info("S1", "Main Street", 53.35, -6.26, None, None, None),
        )]);
        let rows = derive_gtfs_stop_reality(
            &[dataset],
            &[stop_summary("S1", 0, 0, 12)],
            "reality-123",
            "import-123",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reality_status, "school_only_confirmed");
        assert_eq!(rows[0].school_only_state, "yes");
        assert_eq!(rows[0].school_only_departures_30d, 12);
    }

    #[test]
    fn derive_gtfs_stop_reality_prefers_child_platform_over_parent_station() {
        let dataset = dataset_with_stops(vec![
            (
                "PARENT",
                stop_info(
                    "PARENT",
                    "Central Station",
                    53.35,
                    -6.26,
                    None,
                    Some(1),
                    None,
                ),
            ),
            (
                "PLATFORM-1",
                stop_info(
                    "PLATFORM-1",
                    "Central Station Platform 1",
                    53.3501,
                    -6.2601,
                    Some("PARENT"),
                    Some(0),
                    Some("1"),
                ),
            ),
        ]);
        let rows = derive_gtfs_stop_reality(
            &[dataset],
            &[
                stop_summary("PARENT", 12, 48, 0),
                stop_summary("PLATFORM-1", 12, 48, 0),
            ],
            "reality-123",
            "import-123",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].stop_id, "PLATFORM-1");
        assert_eq!(
            rows[0].stop_name.as_deref(),
            Some("Central Station Platform 1")
        );
    }

    #[test]
    fn summarize_gtfs_stops_keeps_tue_sun_when_exception_adds_monday() {
        let analysis_date = Date::from_calendar_date(2026, time::Month::April, 14).unwrap();
        let settings = default_settings(analysis_date);
        let mut dataset = single_bus_dataset("TUESUN", [0, 1, 1, 1, 1, 1, 1], Some(0));
        dataset.calendar_dates.push(CalendarDateException {
            service_id: "TUESUN".to_string(),
            service_date: Date::from_calendar_date(2026, time::Month::April, 13).unwrap(),
            exception_type: 1,
        });
        let service_windows = expand_service_windows(&dataset, &settings);
        let service_classifications =
            classify_services(&dataset, "reality-123", &service_windows);
        let summaries = summarize_gtfs_stops(
            &dataset,
            &settings,
            "reality-123",
            &service_windows,
            &service_classifications,
        );

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].bus_active_days_mask_7d.as_deref(), Some("0111111"));
        assert_eq!(summaries[0].bus_service_subtier.as_deref(), Some("tue_sun"));
        assert!(!summaries[0].has_exception_only_service);
    }

    #[test]
    fn summarize_gtfs_stops_marks_calendar_dates_only_bus_service() {
        let analysis_date = Date::from_calendar_date(2026, time::Month::April, 14).unwrap();
        let settings = default_settings(analysis_date);
        let mut dataset = dataset_with_stops(vec![(
            "S1",
            stop_info("S1", "Main Street", 53.35, -6.26, None, Some(0), None),
        )]);
        dataset.calendar_dates.push(CalendarDateException {
            service_id: "EXC_ONLY".to_string(),
            service_date: analysis_date,
            exception_type: 1,
        });
        dataset.stop_service_occurrences.insert(
            (
                "S1".to_string(),
                "EXC_ONLY".to_string(),
                "R1".to_string(),
                "bus".to_string(),
            ),
            1,
        );
        dataset.service_time_buckets.insert(
            "EXC_ONLY".to_string(),
            TimeBucketCounts {
                morning: 0,
                afternoon: 0,
                offpeak: 1,
            },
        );
        dataset.service_route_ids.insert(
            "EXC_ONLY".to_string(),
            BTreeSet::from(["R1".to_string()]),
        );
        dataset.service_route_modes.insert(
            "EXC_ONLY".to_string(),
            BTreeSet::from(["bus".to_string()]),
        );

        let service_windows = expand_service_windows(&dataset, &settings);
        let service_classifications =
            classify_services(&dataset, "reality-123", &service_windows);
        let summaries = summarize_gtfs_stops(
            &dataset,
            &settings,
            "reality-123",
            &service_windows,
            &service_classifications,
        );

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].bus_active_days_mask_7d.as_deref(), Some("0000000"));
        assert_eq!(summaries[0].bus_service_subtier, None);
        assert!(summaries[0].has_exception_only_service);
    }

    #[test]
    fn summarize_gtfs_stops_buckets_bus_daytime_frequency() {
        let analysis_date = Date::from_calendar_date(2026, time::Month::April, 14).unwrap();
        let settings = default_settings(analysis_date);
        let cases = [
            (56, 15.0, "frequent", 5),
            (28, 30.0, "moderate", 4),
            (14, 60.0, "low_frequency", 3),
            (7, 120.0, "very_low_frequency", 2),
            (6, 140.0, "token_skeletal", 1),
        ];

        for (departures, expected_headway, expected_tier, expected_units) in cases {
            let mut dataset = single_bus_dataset("SVC1", [1, 1, 1, 1, 1, 0, 0], Some(0));
            dataset.stop_service_time_occurrences.insert(
                (
                    "S1".to_string(),
                    "SVC1".to_string(),
                    "R1".to_string(),
                    "bus".to_string(),
                    Some(12 * 3600),
                ),
                departures,
            );
            let service_windows = expand_service_windows(&dataset, &settings);
            let service_classifications =
                classify_services(&dataset, "reality-123", &service_windows);
            let summaries = summarize_gtfs_stops(
                &dataset,
                &settings,
                "reality-123",
                &service_windows,
                &service_classifications,
            );

            assert_eq!(summaries.len(), 1);
            assert_eq!(summaries[0].bus_daytime_deps, f64::from(departures));
            assert_eq!(
                summaries[0].bus_daytime_headway_min.unwrap(),
                expected_headway
            );
            assert_eq!(summaries[0].bus_frequency_tier.as_deref(), Some(expected_tier));
            assert_eq!(summaries[0].bus_frequency_score_units, expected_units);
            assert_eq!(summaries[0].transport_score_units, expected_units);
        }
    }

    #[test]
    fn summarize_gtfs_stops_skips_unscheduled_for_non_boarding_stop() {
        let analysis_date = Date::from_calendar_date(2026, time::Month::April, 14).unwrap();
        let settings = default_settings(analysis_date);
        let dataset = dataset_with_stops(vec![(
            "PARENT",
            stop_info(
                "PARENT",
                "Central Station",
                53.35,
                -6.26,
                None,
                Some(1),
                None,
            ),
        )]);
        let service_windows = expand_service_windows(&dataset, &settings);
        let service_classifications =
            classify_services(&dataset, "reality-123", &service_windows);
        let summaries = summarize_gtfs_stops(
            &dataset,
            &settings,
            "reality-123",
            &service_windows,
            &service_classifications,
        );

        assert!(summaries.is_empty());
    }
}
