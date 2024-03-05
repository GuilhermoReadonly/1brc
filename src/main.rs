use std::{
    collections::HashMap, error::Error, fmt::Display, fs::{self, File}, io::{BufRead, BufReader}, sync::{Arc, Mutex}, thread, time::Instant
};

#[derive(Debug, Clone, Copy)]
struct State {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}

impl Display for State {
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

fn main() -> Result<(), Box<dyn Error>> {
    let stations_stats: HashMap<String, State> = HashMap::new();
    let stations_stats = Arc::new(Mutex::new(stations_stats));
    let cores: usize = std::thread::available_parallelism().unwrap().into();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };

    let now = Instant::now();
    read(cores, path, stations_stats.clone())?;
    let elapsed_time = now.elapsed();
    println!("Running read() took {} ms.", elapsed_time.as_millis());

    let now = Instant::now();
    write_result(stations_stats)?;
    let elapsed_time = now.elapsed();
    println!(
        "Running write_result() took {} ms.",
        elapsed_time.as_millis()
    );

    Ok(())
}

fn write_result(stations_stats: Arc<Mutex<HashMap<String, State>>>) -> Result<(), Box<dyn Error>> {
    print!("{{");

    let s = stations_stats.lock().unwrap();
    let mut station_iter_sorted: Vec<&String> = s.keys().collect();
    station_iter_sorted.sort();

    for station in station_iter_sorted {
        let state = s.get(station).expect("Station must exist");
        print!("{station}={state}, ");
    }
    println!("}}");
    Ok(())
}

fn read_chunk(
    path: String,
    stations_stats: Arc<Mutex<HashMap<String, State>>>,
    _start: u64,
    _size: u64,
) -> Result<(), Box<dyn Error>> {
    println!("{:?}: Start read_chunk", thread::current().id());
    let file = File::open(&path)?;

    let lines = BufReader::new(file).lines();
    for line in lines {
        let line_string = line?;
        let mut splitline = line_string.split(";");
        let station = splitline
            .next()
            .expect("first element is the station")
            .to_string();
        let value = splitline
            .next()
            .expect("second element is the value")
            .parse::<f64>()
            .expect("value can be parsed into f64");

        let s = stations_stats.lock();
        let mut s = s.unwrap();
        let mut current_state_opt = s.get(&station);
        let state = State {
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

        let updated_state = State {
            min: new_min,
            max: new_max,
            count: new_count,
            sum: new_sum,
        };

        s.insert(station, updated_state);
    }
    println!("{:?}: End read_chunk", thread::current().id());
    Ok(())
}

fn read(
    nb_cores: usize,
    path: String,
    stations_stats: Arc<Mutex<HashMap<String, State>>>,
) -> Result<(), Box<dyn Error>> {
    let file_size = fs::metadata(&path)?.len();
    let chunk_size: u64 = file_size / nb_cores as u64;

    for core in 0..nb_cores {
        let stat = Arc::clone(&stations_stats);
        let path = path.clone();
        println!("{:?}: Before spawning in read", thread::current().id());
        let _thread = std::thread::spawn(move || {
            let start = core as u64 * chunk_size;
            read_chunk(path, stat, start, chunk_size).unwrap();
        });
        println!("{:?}: After spawning thread {:?}", thread::current().id(), _thread.thread().id());
    }
    Ok(())
}
