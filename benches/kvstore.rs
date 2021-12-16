use criterion::{criterion_group, criterion_main, Criterion};
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::sled::SledKvsEngine;
use rand::Rng;
use rand::thread_rng;
use rand::distributions::Alphanumeric;
use tempfile::TempDir;

fn criterion_benchmark(c: &mut Criterion) {
	let mut keys:Vec<String> = vec![];
	let mut values: Vec<String> = vec![];
	println!("Init keys and values...");
	for _ in 0..100 {
		let klen = thread_rng().gen_range(1, 100000);
		let vlen = thread_rng().gen_range(1, 100000);
		keys.push(
			thread_rng()
			.sample_iter(&Alphanumeric)
			.take(klen)
			.collect()
		);

		values.push(
			thread_rng()
			.sample_iter(&Alphanumeric)
			.take(vlen)
			.collect()
		);
	}

	println!("Init OK!");
	println!("Create engines...");
	let kvs_dir = TempDir::new().expect("unable to create temporary working directory");
	let mut kvs = KvStore::open(kvs_dir.path()).unwrap();
	let sled_dir = TempDir::new().expect("unable to create temporary working directory");
	let mut sled = SledKvsEngine::open(sled_dir.path()).unwrap();

	println!("Create engines...");
    c.bench_function("kvs write", |b| b.iter(|| {
		for i in 0..100 {
			assert!(kvs.set(keys[i].to_owned(), values[i].to_owned()).is_ok());
		}
	}));

	c.bench_function("kvs read", |b| b.iter(|| {
		for i in 0..100 {
			assert!(kvs.get(keys[i].to_owned()).is_ok());
		}
	}));

	c.bench_function("sled write", |b| b.iter(|| {
		for i in 0..100 {
			sled.set(keys[i].to_owned(), values[i].to_owned()).unwrap();
		}
	}));

	c.bench_function("sled read", |b| b.iter(|| {
		for i in 0..100 {
			sled.get(keys[i].to_owned()).unwrap();
		}
	}));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);