//! Experimental Linux process-resource measurements for multiple open stores.
#[cfg(target_os = "linux")]
#[path = "multi_store_resources/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("multi-store resource measurement requires Linux /proc");
    std::process::exit(1);
}
