#![allow(unused_variables)]

use std::{
    error::Error,
    fmt::Display,
    fs::{self, File},
    io::{BufRead, BufReader},
    thread::{self},
    time::Instant,
};

#[derive(Debug, Clone, Copy)]
struct Stats {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.min,
            self.max,
            self.sum / (self.count as f64)
        )
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: Default::default(),
            sum: Default::default(),
        }
    }
}

// type Map<K, V> = std::collections::HashMap<K, V>;
type Map<K, V> = std::collections::BTreeMap<K, V>;

fn main() -> Result<(), Box<dyn Error>> {
    let mut stations_stats: Map<String, Stats> = Map::new();
    let cores: usize = std::thread::available_parallelism().unwrap().into();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };

    let metadata = fs::metadata(&path)?;
    println!("File size = {}", metadata.len());

    let now = Instant::now();
    read(cores, path, &mut stations_stats)?;
    println!("Running read() took {} us.", now.elapsed().as_micros());

    let now = Instant::now();
    write_result(&stations_stats)?;
    println!(
        "Running write_result() took {} us.",
        now.elapsed().as_micros()
    );

    Ok(())
}

fn read(
    nb_cores: usize,
    path: String,
    stations_stats: &mut Map<String, Stats>,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(&path)?;

    let file = BufReader::new(file);

    let mut size_read = 1;

    let lines = file.lines();

    for (i, line) in lines.enumerate() {
        let line_string = line?;

        size_read += line_string.bytes().len() as u64;

        let (station, value) = line_string.split_once(";").unwrap();
        let value = value.parse()?;

        let mut current_state_opt = stations_stats.get(station);
        let state = Stats {
            min: value,
            max: value,
            count: 1,
            sum: value,
        };
        let current_state = current_state_opt.get_or_insert(&state);

        let new_min = if current_state.min < value {
            current_state.min
        } else {
            value
        };
        let new_max = if current_state.max > value {
            current_state.max
        } else {
            value
        };
        let new_count = current_state.count + 1;
        let new_sum = current_state.sum + value;

        let updated_state = Stats {
            min: new_min,
            max: new_max,
            count: new_count,
            sum: new_sum,
        };

        stations_stats.insert(station.to_string(), updated_state);

        if i % 100_000_000 == 0 {
            println!("{:?}: Read {i}", thread::current().id());
        }
    }
    Ok(())
}

fn write_result(stations_stats: &Map<String, Stats>) -> Result<(), Box<dyn Error>> {
    print!("{{");

    for (station, state) in stations_stats.iter() {
        print!("{station}={state}, ");
    }
    println!("}}");
    Ok(())
}
