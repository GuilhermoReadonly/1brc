use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    fs::{self, File},
    io::{BufRead, BufReader, Seek},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Instant,
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

    let metadata = fs::metadata(&path)?;
    println!("File size = {}", metadata.len());

    let now = Instant::now();
    read(cores, path, stations_stats.clone())?;
    let elapsed_time = now.elapsed();
    println!("Running read() took {} us.", elapsed_time.as_micros());

    let now = Instant::now();
    write_result(stations_stats)?;
    let elapsed_time = now.elapsed();
    println!(
        "Running write_result() took {} us.",
        elapsed_time.as_micros()
    );

    Ok(())
}

fn write_result(stations_stats: Arc<Mutex<HashMap<String, State>>>) -> Result<(), Box<dyn Error>> {
    print!("{{");

    let s = stations_stats.lock().unwrap();
    let mut station_iter_sorted: Vec<&String> = s.keys().collect();
    station_iter_sorted.sort();

    for station in station_iter_sorted {
        let _state = s.get(station).expect("Station must exist");
        print!("{station}={_state}, ");
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
    let mut file = File::open(&path)?;

    file.seek(std::io::SeekFrom::Start(_start))?;


    let lines = BufReader::new(file).lines();

    let mut size_read = 0;
    for line in lines {

        if size_read >= _size{
            break;
        }
        let line_string = line?;

        size_read += line_string.bytes().len() as u64;

        let splitline: Vec<&str> = line_string.split(";").collect();
        if splitline.len() != 2{
            println!("{:?}: After {size_read} read from {_start}, the line is malformed: {line_string:?}", thread::current().id());
            continue;
        }

        let station = splitline[0];
        let value = splitline[1].parse()?;

        let s = stations_stats.lock();
        let mut s = s.unwrap();
        let mut current_state_opt = s.get(station);
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

        s.insert(station.to_string(), updated_state);
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

    let mut handles: Vec<JoinHandle<()>> = vec![];

    for core in 0..nb_cores {
        let stat = Arc::clone(&stations_stats);
        let path = path.clone();
        println!("{:?}: Before spawning in read", thread::current().id());
        let _thread = std::thread::spawn(move || {
            let start = core as u64 * chunk_size;
            match read_chunk(path, stat, start, chunk_size) {
                Err(e) => println!("{:?}: Error : {e}", thread::current().id()),
                _ => println!("{:?}: Finished", thread::current().id()),
            };
        });

        println!(
            "{:?}: After spawning thread {:?}",
            thread::current().id(),
            _thread.thread().id()
        );

        handles.push(_thread);
    }

    for child in handles {
        // Wait for the threads to finish. Returns a result.
        let _ = child.join();
    }
    Ok(())
}
