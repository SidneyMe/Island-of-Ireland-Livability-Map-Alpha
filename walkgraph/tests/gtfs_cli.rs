use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::json;
use tempfile::TempDir;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

fn walkgraph_bin() -> &'static str {
    env!("CARGO_BIN_EXE_walkgraph")
}

fn write_gtfs_zip(path: &Path) {
    let file = fs::File::create(path).expect("create gtfs zip");
    let mut zip = ZipWriter::new(file);
    let options = SimpleFileOptions::default();
    let files = [
        (
            "stops.txt",
            "stop_id,stop_code,stop_name,stop_lat,stop_lon\nS1,1001,Main Street,53.3500,-6.2600\n",
        ),
        (
            "routes.txt",
            "route_id,route_short_name,route_long_name,route_type\nR1,42,School Shuttle,3\n",
        ),
        (
            "trips.txt",
            "route_id,service_id,trip_id,trip_headsign\nR1,SVC1,T1,Campus\n",
        ),
        (
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:05:00,08:05:00,S1,1\n",
        ),
        (
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nSVC1,1,1,1,1,1,0,0,20260401,20260430\n",
        ),
        (
            "calendar_dates.txt",
            "service_id,date,exception_type\nSVC1,20260414,1\n",
        ),
    ];
    for (name, content) in files {
        zip.start_file(name, options).expect("start zip member");
        std::io::Write::write_all(&mut zip, content.as_bytes()).expect("write zip member");
    }
    zip.finish().expect("finish zip");
}

#[test]
fn gtfs_refresh_command_writes_expected_artifacts() {
    let temp_dir = TempDir::new().expect("temp dir");
    let zip_path = temp_dir.path().join("feed.zip");
    let config_path = temp_dir.path().join("config.json");
    let out_dir = temp_dir.path().join("out");

    write_gtfs_zip(&zip_path);
    fs::write(
        &config_path,
        serde_json::to_string_pretty(&json!({
            "analysis_date": "2026-04-14",
            "analysis_window_days": 30,
            "service_desert_window_days": 7,
            "lookahead_days": 14,
            "import_fingerprint": "import-123",
            "reality_fingerprint": "reality-123",
            "school_keywords": ["school"],
            "school_am_start_hour": 6,
            "school_am_end_hour": 10,
            "school_pm_start_hour": 13,
            "school_pm_end_hour": 17,
            "feeds": [{
                "feed_id": "nta",
                "label": "NTA",
                "zip_path": zip_path,
                "feed_fingerprint": "feed-fingerprint-123",
                "source_url": null
            }]
        }))
        .expect("serialize config"),
    )
    .expect("write config");

    let output = Command::new(walkgraph_bin())
        .arg("gtfs-refresh")
        .arg("--config-json")
        .arg(&config_path)
        .arg("--out-dir")
        .arg(&out_dir)
        .output()
        .expect("run gtfs-refresh");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let summary_path = out_dir.join("run_summary.json");
    let stop_times_path = out_dir.join("raw").join("nta").join("stop_times.csv");
    let reality_path = out_dir.join("derived").join("gtfs_stop_reality.csv");

    assert!(summary_path.exists());
    assert!(stop_times_path.exists());
    assert!(reality_path.exists());

    let summary_text = fs::read_to_string(summary_path).expect("read summary");
    assert!(summary_text.contains("\"feed_id\": \"nta\""));
    assert!(summary_text.contains("\"gtfs_stop_reality\""));
    assert!(!summary_text.contains("\"stop_matches\""));

    let stop_times_text = fs::read_to_string(stop_times_path).expect("read stop_times");
    assert!(stop_times_text.contains("T1,29100,29100,S1,1"));

    let reality_text = fs::read_to_string(reality_path).expect("read reality csv");
    assert!(reality_text.contains("source_status"));
    assert!(reality_text.contains("gtfs_direct"));
    assert!(reality_text.contains("school_only_confirmed"));
    assert!(reality_text.contains("\"school_only_departures_present\""));
}
