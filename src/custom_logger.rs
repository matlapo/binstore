use chrono::prelude::*;
use env_logger::{Env, Builder, fmt};
use log::Level;

use std::io::Write;

pub fn init() {
    let env = Env::default();

    let mut builder = Builder::from_env(env);

    builder.format(|buf, record| {
        let now_str = Local::now().format("%Y-%m-%d %H:%M:%S");

        let mut style = buf.style();

        let color = match record.level() {
            Level::Info => fmt::Color::Green,
            Level::Warn => fmt::Color::Yellow,
            Level::Error => fmt::Color::Red,
            Level::Debug => fmt::Color::Magenta,
            Level::Trace => fmt::Color::Blue,
        };

        style.set_color(color).set_bold(true);
        let log_level = style.value(record.level());

        writeln!(
            buf, "{:5} {} {:?}",
            log_level,
            now_str,
            record.args())
    });

    builder.init();
}
