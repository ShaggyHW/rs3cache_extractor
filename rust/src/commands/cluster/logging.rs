use env_logger::Env;

pub fn init(level: Option<&str>) {
    let default = level.unwrap_or("info");
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or(default))
        .try_init();
}
