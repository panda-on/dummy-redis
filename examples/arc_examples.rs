use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    // This variable declaration is where its value is specified.
    let apple = Arc::new("the same apple");

    let mut handles = vec![];
    for _ in 0..10 {
        // Here there is no value specification as it is a pointer to a
        // reference in the memory heap.
        let apple = Arc::clone(&apple);
        let handle = thread::spawn(move || {
            // As Arc was used, rust can spawn threads using allocated
            // value referred to Arc variable pointer's location.
            println!("{:?}", apple);
        });
        handles.push(handle);
    }

    // Make sure all Arc instances are printed from spawned threads.
    thread::sleep(Duration::from_secs(1));
}
