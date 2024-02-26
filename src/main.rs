
use std::{fmt::Display, collections::HashMap, error::Error, fs::File, io::{BufRead, BufReader}};

#[derive(Debug, Clone, Copy)]
struct State {
    min: f64,
    max: f64,
    count: u64,
    sum: f64,
}

impl Display for State{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.min, self.max, self.sum/(self.count as f64))
    }
}


fn main() -> Result<(), Box<dyn Error>> {
    let mut stations_stats : HashMap<String, State> = HashMap::new();
    // let cores: usize = std::thread::available_parallelism().unwrap().into();

    let path = match std::env::args().skip(1).next() {
        Some(path) => path,
        None => "measurements.txt".to_owned(),
    };

    let file = File::open(path)?;
    let lines = BufReader::new(file).lines();

    for line in lines {
        let line_string = line?;
        let mut splitline = line_string.split(";");
        let station = splitline.next().expect("first element is the station").to_string();
        let value = splitline.next().expect("second element is the value").parse::<f64>().expect("value can be parsed into f64");

        let mut current_state_opt = stations_stats.get(&station);
        let state = State { min: value, max: value, count: 1, sum: value};
        let current_state = current_state_opt.get_or_insert(&state);

        let new_min = if current_state.min < value {current_state.min} else {value};
        let new_max = if current_state.max > value {current_state.max} else {value};
        let new_count = current_state.count + 1;
        let new_sum = current_state.sum + value;

        let updated_state = State { min: new_min, max: new_max, count: new_count, sum: new_sum};

        stations_stats.insert(station, updated_state);

    };

    print!("{{");

    let mut station_iter_sorted: Vec<&String> = stations_stats.keys().collect();
    station_iter_sorted.sort();
    
    for station in station_iter_sorted{
        let state = stations_stats.get(station).expect("Station must exist");
        print!("{station}={state},");
    }
    print!("}}");

    Ok(())
}
